# Redpanda

Redpanda is the Kafka-compatible broker between the [producer](api/producer.md) and the [stream processor](processor.md). The producer publishes market observations to `market-observations`; Flink reads that topic. The [consumer](api/consumer.md) is a separate debugging tool that can inspect the same messages. Redpanda Console provides a browser view of topics and messages.

## Start and check the services

From the repository root, validate the Compose configuration and start Redpanda with its Console:

```powershell
docker compose -f infra/compose.yml config
docker compose -f infra/compose.yml up -d redpanda console
docker compose -f infra/compose.yml ps
```

Open [Redpanda Console](http://localhost:8080/overview) to inspect the broker. The full Compose stack also starts Flink; see the [stream processor](processor.md) page for its setup.

## Create and inspect the topic

The project uses `market-observations` for input and `market.alerts` for price-drop alerts. The input topic has three partitions so Flink can read it in parallel. Alerts are infrequent here, so the alert topic needs only one partition. Both topics have replication factor one because this local setup has one broker. Create them once, then list or describe them to confirm:

```powershell
docker exec eve-redpanda rpk topic create market-observations -p 3 -r 1
docker exec eve-redpanda rpk topic create market.alerts -p 1 -r 1
docker exec eve-redpanda rpk topic list
docker exec eve-redpanda rpk topic describe market-observations
```

If a topic already exists, skip its create command. The existing `market.alerts` topic may have three partitions from the earlier setup; it works as is. Redpanda cannot reduce a topic's partition count in place. Broker data is stored in the Docker volume `redpanda-data`, so it survives container recreation while that volume is retained.

## Connection addresses

| Client | Broker address | Why |
| --- | --- | --- |
| Producer or debugging consumer running on Windows | `localhost:19092` | Compose publishes this port to the host. |
| Flink running in Compose | `redpanda:9092` | Containers use the internal Compose network. |
| Browser | [localhost:8080](http://localhost:8080/overview) | Redpanda Console web UI. |

The producer's packaged defaults already use `localhost:19092` and `market-observations`. If you copy `producer/.env.example` to `producer/.env`, set `KAFKA_BROKER=localhost:19092` and `KAFKA_TOPIC=market-observations` there; the example currently contains different values and overrides those defaults.

## Verify a message

From `producer/`, publish the included sample event without calling the mock API:

```powershell
poetry run eve-producer --kafka-only --event-file examples/sample_event.json
```

Then inspect the topic in Console or read one message from the beginning:

```powershell
docker exec eve-redpanda rpk topic consume market-observations -n 1 -o start
```

The consumer can also print and validate records. From `consumer/`, use a new group name when you want to start at the earliest retained offsets:

```powershell
poetry run eve-consumer --group docs-check --from-beginning --max-messages 1 --validate
```

`--from-beginning` only applies to a group without committed offsets. Choose another group name for a later replay. See `consumer/README.md` for its offset and commit behavior.

After the processor has enough history and sees a qualifying drop, inspect its output in Console or run:

```powershell
docker exec eve-redpanda rpk topic consume market.alerts -n 1 -o start
```
