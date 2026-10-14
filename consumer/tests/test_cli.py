"""Deterministic tests for the debugging consumer."""

import contextlib
import io
import json
import sys
import unittest
from pathlib import Path
from sys import path
from unittest.mock import MagicMock, call, patch

from confluent_kafka import TopicPartition

path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eve_consumer import cli


VALID_EVENT = {
    "item_id": 34,
    "item_name": "Tritanium",
    "current_price": 4.5,
    "avg_price": 5.0,
    "price_change": -10.0,
    "volume": 100,
    "source_timestamp": "2026-01-01T00:00:00Z",
    "location": {"region_id": 10000002, "region_name": "The Forge"},
    "fetched_at": "2026-01-01T00:00:01Z",
    "event_timestamp": "2026-01-01T00:00:02Z",
}


def make_message(payload: object = VALID_EVENT, offset: int = 7) -> MagicMock:
    """Build a Kafka message stub with stable metadata.

    Args:
        payload: JSON value for the message body.
        offset: Kafka offset reported by the stub.

    Returns:
        A message-shaped mock.
    """
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = json.dumps(payload).encode("utf-8")
    message.key.return_value = b"10000002:34"
    message.topic.return_value = "market-observations"
    message.partition.return_value = 2
    message.offset.return_value = offset
    message.timestamp.return_value = (1, 1767225602000)
    return message


class ConsumerTests(unittest.TestCase):
    """Verify output, offset behavior, and shutdown with a fake Kafka client."""

    @patch.object(cli, "Consumer")
    def test_logs_assigned_partitions_on_group_join(self, consumer_class: MagicMock) -> None:
        """The assignment callback reports every assigned partition in order."""
        consumer = consumer_class.return_value
        consumer.poll.return_value = make_message()

        with contextlib.redirect_stdout(io.StringIO()):
            cli.consume(max_messages=1)

        consumer.subscribe.assert_called_once()
        self.assertEqual(consumer.subscribe.call_args.args, ([cli.DEFAULT_TOPIC],))
        callback = consumer.subscribe.call_args.kwargs["on_assign"]
        partitions = [
            TopicPartition(cli.DEFAULT_TOPIC, 2),
            TopicPartition(cli.DEFAULT_TOPIC, 0),
            TopicPartition(cli.DEFAULT_TOPIC, 1),
        ]
        with self.assertLogs("eve_consumer.cli", level="INFO") as logs:
            callback(consumer, partitions)

        self.assertEqual(
            logs.output,
            ["INFO:eve_consumer.cli:Assigned partitions:\n"
             "  market-observations[0]\n"
             "  market-observations[1]\n"
             "  market-observations[2]"],
        )

    @patch.object(cli, "Consumer")
    def test_prints_metadata_and_commits_each_processed_message(self, consumer_class: MagicMock) -> None:
        """Successful processing prints metadata before a synchronous commit."""
        consumer = consumer_class.return_value
        first, second = make_message(offset=7), make_message(offset=8)
        consumer.poll.side_effect = [None, first, second]
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            count = cli.consume(max_messages=2, from_beginning=True, validate=True)

        self.assertEqual(count, 2)
        self.assertEqual(consumer.poll.call_count, 3)
        self.assertTrue(all(call.args == (cli.POLL_TIMEOUT_SECONDS,) for call in consumer.poll.call_args_list))
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([record["offset"] for record in records], [7, 8])
        self.assertEqual(records[0]["partition"], 2)
        self.assertEqual(records[0]["timestamp"], "2026-01-01T00:00:02+00:00")
        self.assertEqual(records[0]["key"], "10000002:34")
        self.assertEqual(records[0]["payload"], VALID_EVENT)
        self.assertEqual(consumer.commit.call_count, 2)
        consumer.commit.assert_any_call(message=first, asynchronous=False)
        consumer.commit.assert_any_call(message=second, asynchronous=False)
        consumer.close.assert_called_once_with()
        self.assertEqual(consumer_class.call_args.args[0]["auto.offset.reset"], "earliest")
        self.assertIs(consumer_class.call_args.args[0]["enable.auto.commit"], False)
        self.assertIs(consumer_class.call_args.args[0]["enable.auto.offset.store"], False)

    @patch.object(cli, "Consumer")
    def test_no_commit_leaves_group_offsets_untouched(self, consumer_class: MagicMock) -> None:
        """The replay option does not call any offset advancing operation."""
        consumer = consumer_class.return_value
        consumer.poll.return_value = make_message()

        with contextlib.redirect_stdout(io.StringIO()):
            cli.consume(group="replay", no_commit=True, max_messages=1)

        consumer.commit.assert_not_called()
        consumer.store_offsets.assert_not_called()
        self.assertEqual(consumer_class.call_args.args[0]["group.id"], "replay")
        self.assertEqual(consumer_class.call_args.args[0]["auto.offset.reset"], "latest")

    @patch.object(cli, "Consumer")
    def test_invalid_event_is_not_printed_or_committed(self, consumer_class: MagicMock) -> None:
        """Validation failure logs its location and leaves the message uncommitted."""
        consumer = consumer_class.return_value
        consumer.poll.return_value = make_message({"item_id": 34})
        output = io.StringIO()

        with self.assertLogs("eve_consumer.cli", level="ERROR") as logs:
            with contextlib.redirect_stdout(output), self.assertRaisesRegex(ValueError, "item_name"):
                cli.consume(validate=True, max_messages=1)

        self.assertEqual(output.getvalue(), "")
        self.assertEqual(
            logs.output,
            ["ERROR:eve_consumer.cli:Validation failed: "
             "topic=market-observations partition=2 offset=7 "
             "key=b'10000002:34': item_name must be a str"],
        )
        consumer.commit.assert_not_called()
        consumer.close.assert_called_once_with()

    @patch.object(cli, "Consumer")
    def test_bad_json_is_not_committed_without_validation(self, consumer_class: MagicMock) -> None:
        """Even without schema checks, the message must contain JSON."""
        consumer = consumer_class.return_value
        message = make_message()
        message.value.return_value = b"not JSON"
        consumer.poll.return_value = message

        with self.assertLogs("eve_consumer.cli", level="ERROR") as logs:
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(ValueError):
                cli.consume(max_messages=1)

        self.assertIn("Message decoding failed: topic=market-observations partition=2 offset=7", logs.output[0])
        consumer.commit.assert_not_called()
        consumer.close.assert_called_once_with()

    @patch.object(cli, "Consumer")
    def test_skip_invalid_commits_bad_record_and_continues(self, consumer_class: MagicMock) -> None:
        """Explicit skip mode advances past a bad event and prints the next good one."""
        consumer = consumer_class.return_value
        invalid = make_message({**VALID_EVENT, "volume": "unknown"}, offset=7)
        valid = make_message(offset=8)
        consumer.poll.side_effect = [invalid, valid]
        output = io.StringIO()

        with self.assertLogs("eve_consumer.cli", level="ERROR") as logs:
            with contextlib.redirect_stdout(output):
                count = cli.consume(validate=True, skip_invalid=True, max_messages=1)

        self.assertEqual(count, 1)
        self.assertEqual([json.loads(line)["offset"] for line in output.getvalue().splitlines()], [8])
        self.assertEqual(
            logs.output,
            ["ERROR:eve_consumer.cli:Validation failed: "
             "topic=market-observations partition=2 offset=7 "
             "key=b'10000002:34': volume must be a int"],
        )
        self.assertEqual(
            consumer.commit.call_args_list,
            [call(message=invalid, asynchronous=False),
             call(message=valid, asynchronous=False)],
        )
        consumer.close.assert_called_once_with()

    @patch.object(cli, "Consumer")
    def test_skip_invalid_with_no_commit_never_advances_offsets(self, consumer_class: MagicMock) -> None:
        """No-commit mode keeps offsets untouched even while skipping failures."""
        consumer = consumer_class.return_value
        consumer.poll.side_effect = [make_message({"item_id": 34}), make_message()]

        with self.assertLogs("eve_consumer.cli", level="ERROR"):
            with contextlib.redirect_stdout(io.StringIO()):
                cli.consume(validate=True, skip_invalid=True, no_commit=True, max_messages=1)

        consumer.commit.assert_not_called()
        consumer.store_offsets.assert_not_called()

    @patch.object(cli, "consume")
    def test_cli_forwards_skip_invalid(self, consume_mock: MagicMock) -> None:
        """The requested CLI flags reach the consumer loop."""
        with patch.object(sys, "argv", [
            "eve-consumer", "--group", "scaling-test", "--from-beginning",
            "--validate", "--skip-invalid",
        ]):
            cli.main()

        self.assertEqual(consume_mock.call_args.kwargs["group"], "scaling-test")
        self.assertTrue(consume_mock.call_args.kwargs["from_beginning"])
        self.assertTrue(consume_mock.call_args.kwargs["validate"])
        self.assertTrue(consume_mock.call_args.kwargs["skip_invalid"])

    @patch.object(cli, "Consumer")
    def test_interrupt_closes_consumer(self, consumer_class: MagicMock) -> None:
        """Ctrl+C during a bounded poll still releases the Kafka client."""
        consumer = consumer_class.return_value
        consumer.poll.side_effect = KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            cli.consume()

        consumer.close.assert_called_once_with()

    def test_validation_rejects_boolean_for_numeric_field(self) -> None:
        """JSON booleans cannot stand in for integer market fields."""
        event = {**VALID_EVENT, "item_id": True}
        with self.assertRaisesRegex(ValueError, "item_id"):
            cli.validate_event(event)


if __name__ == "__main__":
    unittest.main()
