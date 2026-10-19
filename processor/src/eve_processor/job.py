"""Continuous PyFlink DataStream job for rolling market alerts."""

import json
from datetime import datetime, timezone

from pyflink.common import Configuration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import KeyedProcessFunction, StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from .config import ProcessorConfig
from .latency import LATENCY_BUCKET_SECONDS, classify_latency, is_sla_success
from .models import MarketObservation
from .processing import classify
from .rolling import RollingState, evaluate, rolling_key


def build_source(config: ProcessorConfig) -> KafkaSource:
    """Build a source that discovers and reads every topic partition."""
    offsets = {
        "earliest": KafkaOffsetsInitializer.earliest,
        "latest": KafkaOffsetsInitializer.latest,
        "committed": lambda: KafkaOffsetsInitializer.committed_offsets(
            KafkaOffsetResetStrategy.EARLIEST
        ),
    }[config.start_from]()
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(config.topic)
        .set_group_id(config.group_id)
        .set_starting_offsets(offsets)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def valid_log_line(payload: str) -> str:
    """Format selected observation fields for TaskManager stdout."""
    observation = MarketObservation.model_validate_json(payload)
    return (
        f"VALID region={observation.location.region_id} item={observation.item_id} "
        f"name={observation.item_name!r} price={observation.current_price} "
        f"volume={observation.volume} event_time={observation.event_timestamp.isoformat()}"
    )


class ClassifyWithMetrics(MapFunction):
    """Count malformed timestamps before invalid records leave the stream."""

    def open(self, runtime_context) -> None:
        group = runtime_context.get_metrics_group()
        self.invalid_timestamps = group.counter("observation_invalid_timestamps_total")
        self.invalid_other = group.counter("observation_invalid_other_total")

    def map(self, raw: str) -> tuple[str, str]:
        result = classify(raw)
        if result[0] == "invalid_timestamp":
            self.invalid_timestamps.inc()
        elif result[0] == "invalid":
            self.invalid_other.inc()
        return result


class RollingAlertOperator(KeyedProcessFunction):
    """Keep one bounded rolling baseline per Flink market key."""

    def __init__(self, config: ProcessorConfig) -> None:
        """Keep the evaluated runtime policy for this operator instance."""
        self.config = config
        self.state = None

    def open(self, runtime_context) -> None:
        """Acquire checkpointed state with an idle-key processing-time TTL."""
        descriptor = ValueStateDescriptor("rolling-market-state-v2", Types.STRING())
        descriptor.enable_time_to_live(
            StateTtlConfig.new_builder(Time.hours(self.config.window_hours + 24)).build()
        )
        self.state = runtime_context.get_state(descriptor)
        group = runtime_context.get_metrics_group()
        self.live_total = group.counter("observation_live_processed_total")
        self.within_sla = group.counter("observation_within_5s_total")
        self.backfill_total = group.counter("observation_backfill_total")
        self.replay_total = group.counter("observation_replay_total")
        self.future_total = group.counter("observation_future_total")
        self.stale_total = group.counter("observation_stale_total")
        # These are explicit cumulative counters with a Prometheus `le` label.
        # PyFlink distributions do not provide aggregatable rolling buckets.
        self.latency_buckets = [
            (bound, group.add_group("le", str(bound)).counter("observation_latency_seconds_bucket"))
            for bound in LATENCY_BUCKET_SECONDS
        ]
        self.latency_inf = group.add_group("le", "+Inf").counter(
            "observation_latency_seconds_bucket"
        )

    def process_element(self, payload: str, ctx):
        """Evaluate and update state; emit alerts and bounded diagnostics."""
        observation = MarketObservation.model_validate_json(payload)
        result = evaluate(
            observation,
            RollingState.from_json(self.state.value()),
            self.config,
            datetime.now(timezone.utc),
        )
        if result.status in {"accepted", "late"}:
            self.state.update(result.state.to_json())
        sample = classify_latency(observation, result.status, datetime.now(timezone.utc), self.config)
        if sample.category == "backfill":
            self.backfill_total.inc()
        elif sample.category == "replay":
            self.replay_total.inc()
        elif sample.category == "future":
            self.future_total.inc()
        else:
            self.live_total.inc()
            if result.status == "stale":
                self.stale_total.inc()
            if is_sla_success(sample, result.status):
                self.within_sla.inc()
            for bound, counter in self.latency_buckets:
                if sample.seconds <= bound:
                    counter.inc()
            self.latency_inf.inc()
        if result.alert is not None:
            yield "alert", json.dumps(result.alert, separators=(",", ":"), allow_nan=False)
        elif result.status != "accepted":
            yield "diagnostic", (
                f"SKIPPED reason={result.status} region={observation.location.region_id} "
                f"item={observation.item_id} event_time={observation.event_timestamp.isoformat()}"
            )


def build_alert_sink(config: ProcessorConfig) -> KafkaSink:
    """Publish JSON alerts with checkpoint-aware at-least-once delivery."""
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(config.alert_topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_record_serializer(serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def build_job(config: ProcessorConfig) -> StreamExecutionEnvironment:
    """Build validation, keyed rolling state, and alert publication."""
    flink_config = Configuration()
    flink_config.set_integer("python.fn-execution.bundle.time", config.python_bundle_time_ms)
    env = StreamExecutionEnvironment.get_execution_environment(flink_config)
    env.set_parallelism(config.parallelism)
    env.enable_checkpointing(config.checkpoint_interval_ms)
    source = env.from_source(build_source(config), WatermarkStrategy.no_watermarks(), "market-observations")
    classified = source.map(ClassifyWithMetrics(), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
    classified.filter(lambda record: record[0] != "valid").map(
        lambda record: f"INVALID observation: {record[1]}", output_type=Types.STRING()
    ).print()
    valid = classified.filter(lambda record: record[0] == "valid").map(
        lambda record: record[1], output_type=Types.STRING()
    )
    valid.map(valid_log_line, output_type=Types.STRING()).print()
    dimension_types = {
        "station_id": Types.INT(), "market_side": Types.STRING(), "order_type": Types.STRING(),
    }
    processed = valid.key_by(
        lambda payload: rolling_key(MarketObservation.model_validate_json(payload), config.key_dimensions),
        key_type=Types.TUPLE([Types.INT(), Types.INT()] + [
            dimension_types[dimension] for dimension in config.key_dimensions
        ]),
    ).process(
        RollingAlertOperator(config), output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )
    processed.filter(lambda record: record[0] == "diagnostic").map(
        lambda record: record[1], output_type=Types.STRING()
    ).print()
    processed.filter(lambda record: record[0] == "alert").map(
        lambda record: record[1], output_type=Types.STRING()
    ).sink_to(build_alert_sink(config)).name("market-alerts")
    return env


def main() -> None:
    """Submit the continuously running market observation job."""
    build_job(ProcessorConfig.from_env()).execute("eve-market-alerts-v2")


if __name__ == "__main__":
    main()
