"""Continuous PyFlink DataStream job for market observations."""

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaSource,
)

from .config import ProcessorConfig
from .models import MarketObservation
from .processing import classify, market_key


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


def build_job(config: ProcessorConfig) -> StreamExecutionEnvironment:
    """Build the V1 validation and keyed logging pipeline."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.parallelism)
    env.enable_checkpointing(config.checkpoint_interval_ms)
    source = env.from_source(build_source(config), WatermarkStrategy.no_watermarks(), "market-observations")
    classified = source.map(classify, output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
    classified.filter(lambda record: record[0] == "invalid").map(
        lambda record: f"INVALID observation: {record[1]}", output_type=Types.STRING()
    ).print()
    valid = classified.filter(lambda record: record[0] == "valid").map(
        lambda record: record[1], output_type=Types.STRING()
    )
    valid.key_by(
        lambda payload: market_key(MarketObservation.model_validate_json(payload)),
        key_type=Types.TUPLE([Types.INT(), Types.INT()]),
    ).map(valid_log_line, output_type=Types.STRING()).print()
    return env


def main() -> None:
    """Submit the continuously running market observation job."""
    build_job(ProcessorConfig.from_env()).execute("eve-market-observations-v1")


if __name__ == "__main__":
    main()
