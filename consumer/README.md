# EVE market debugging consumer

This small synchronous utility prints messages from Redpanda as JSON Lines. Each line contains the Kafka partition, offset, timestamp (UTC, or `null` if unavailable), key, and decoded JSON payload. It is independent of the producer and is intended for debugging and learning, not market analysis or stream processing.

## Install and run

Start Redpanda as described in [infra/README.md](../infra/README.md), then run from `consumer/`:

```powershell
poetry install
poetry run eve-consumer --from-beginning --max-messages 10 --validate
```

The defaults are broker `localhost:19092`, topic `market-observations`, and group `eve-debug-consumer`. Use `--broker`, `--topic`, and `--group` to change them. `--validate` checks the required market-event fields and their basic types. All messages must contain valid UTF-8 JSON. By default, an invalid message logs its topic, partition, offset, key, and error, then stops the consumer with a nonzero exit without committing that message.

Use tolerant validation to inspect a stream containing known bad observations:

```powershell
poetry run eve-consumer --group scaling-test --from-beginning --validate --skip-invalid
```

With `--skip-invalid`, the consumer logs each invalid message, commits past it, and continues. Invalid messages do not count toward `--max-messages`, which counts printed messages. `--no-commit` still suppresses all commits when used with `--skip-invalid`. Without `--validate`, the skip option applies only to messages that cannot be decoded as JSON.

When the consumer joins a group or its assignment changes, it logs the assigned partitions at INFO level on stderr. Message JSON Lines remain on stdout. For a consumer assigned all three partitions, the log looks like:

```text
INFO Assigned partitions:
  market-observations[0]
  market-observations[1]
  market-observations[2]
```

## Replay and offset commits

Each successfully printed message is committed synchronously. Invalid messages are committed only with `--skip-invalid` (unless `--no-commit` is set). The group ID is persistent, so another run with the same group resumes after its committed offsets:

```powershell
poetry run eve-consumer --group investigation-1 --from-beginning --max-messages 5
poetry run eve-consumer --group investigation-1 --max-messages 5
```

`--from-beginning` applies **only when the group has no existing committed offsets**. To replay from the start, choose a new group ID. `--no-commit` reads without advancing stored group offsets; a later run with the same group can read those messages again:

```powershell
poetry run eve-consumer --group replay-1 --from-beginning --no-commit --max-messages 5
poetry run eve-consumer --group replay-1 --from-beginning --no-commit --max-messages 5
```

If a group already has committed offsets, `--no-commit` starts at those offsets. The consumer disables both automatic commits and automatic offset storage. Ctrl+C closes it after the current bounded poll (at most 0.5 seconds under normal operation).

Offsets are tracked separately for each partition, and `--max-messages` limits the total messages printed, not messages per partition. A few short runs can therefore all show the same partition even when all partitions contain data. The producer uses `region_id:item_id` as its key, so repeated sends of `producer/examples/sample_event.json` also go to the same partition. To inspect older records across the topic, use a fresh group with `--from-beginning --no-commit`, let it run long enough to observe the partition fields, then press Ctrl+C.

## Consumer-group partition assignment

Open two terminals in `consumer/` and use the same group ID:

```powershell
poetry run eve-consumer --group shared-debug --from-beginning
```

Kafka assigns each partition to at most one member of that group at a time. With the three-partition topic created in `infra/README.md`, the two processes divide the partitions; compare the `partition` fields in their output. A third process with the same group shares those three partitions. A fourth process may wait without receiving messages. Different group IDs read independently.

## Tests

```powershell
poetry run python -m unittest discover -s tests -v
```
