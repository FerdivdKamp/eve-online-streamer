# Quickstart

Run these commands in PowerShell from the repository root unless a step says to change directories. Start Docker Desktop first. The producer, consumer, and docs server use Poetry; install each package's dependencies once before its first run.

## 1. Start the mock market API

On the first run, create a named container:

```powershell
docker run -d --name eve-mock-api --restart unless-stopped -p 8000:8000 ghcr.io/bigdatarepublic/bdr-task-api:v-0.0.3
```

After a restart, check it with `docker ps --filter name=eve-mock-api`. If it exists but is stopped, run `docker start eve-mock-api`. The API's Swagger UI is at <http://localhost:8000/>. If the image cannot be pulled, follow the GitHub Container Registry login steps in the repository's `README.md`.

## 2. Start Redpanda and Flink

```powershell
docker compose -f infra/compose.yml up -d
docker compose -f infra/compose.yml ps
```

For the first run, use `docker compose -f infra/compose.yml up -d --build` to build the Flink image. Redpanda Console is at <http://localhost:8080/overview>; the Flink Web UI is at <http://localhost:8081/>.

Check whether the topic exists:

```powershell
docker exec eve-redpanda rpk topic list
```

If `market-observations` is missing, create it:

```powershell
docker exec eve-redpanda rpk topic create market-observations -p 3 -r 1
```

See [Redpanda](redpanda.md) for connection addresses and message checks.

## 3. Submit the Flink job

List running jobs first:

```powershell
docker compose -f infra/compose.yml exec jobmanager flink list
```

If the market job is not already running, submit it once:

```powershell
docker compose -f infra/compose.yml exec jobmanager flink run -d -py /opt/processor/run_job.py
```

Check the job in the Flink Web UI. To follow its output in a separate terminal:

```powershell
docker compose -f infra/compose.yml logs -f taskmanager
```

Submitting the command again starts another job. See [Stream processor](processor.md) for checkpoints, replay, and stopping with a savepoint.

## 4. Run the producer

In a new PowerShell terminal, from the repository root:

```powershell
Set-Location producer
poetry install
poetry run eve-producer --once --max-events 50
```

This fetches up to 50 observations from the mock API and publishes them to Redpanda, then exits. To keep polling, run `poetry run eve-producer --rate 20` from `producer/` and stop it with Ctrl+C. The packaged defaults point to `localhost:8000` for the API and `localhost:19092` for Redpanda. If you create `producer/.env` from `.env.example`, change its broker to `localhost:19092` and topic to `market-observations` before running.

## 5. Inspect messages with the consumer

In another PowerShell terminal, from the repository root:

```powershell
Set-Location consumer
poetry install
poetry run eve-consumer --group quickstart-demo --from-beginning --max-messages 10 --validate --skip-invalid
```

The consumer prints Kafka metadata and JSON payloads, then exits after ten valid messages. `--skip-invalid` lets it pass any older malformed test records in the topic. To replay from the beginning later, choose a new group name: `--from-beginning` does not override a group's committed offsets.

## 6. Start the documentation server

In another PowerShell terminal, from the repository root:

```powershell
poetry -C docs install
poetry -C docs run mkdocs serve -f ../mkdocs.yml --dev-addr 127.0.0.1:8001
```

Open <http://127.0.0.1:8001/>. The documentation server is optional for running the data pipeline.
