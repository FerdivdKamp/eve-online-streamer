# Quickstart

Run these commands in PowerShell from the repository root unless a step says to change directories. Start Docker Desktop first. The producer, consumer, and docs server use Poetry; install each package's dependencies once before its first run.

## 1. Start the stack

Compose starts the mock market API, demo scenario service, Redpanda, Flink, Prometheus, and Grafana:

```powershell
docker compose -f infra/compose.yml up -d --build
```

The API's Swagger UI is at <http://localhost:8000/>. If the image cannot be pulled, follow the GitHub Container Registry login steps in the repository's `README.md`. If an older standalone `eve-mock-api` container already binds port 8000, stop it once with `docker stop eve-mock-api` before starting Compose.

## 2. Check the streaming stack and monitoring

```powershell
docker compose -f infra/compose.yml ps
```

The first step builds the Flink image, creates the two topics, and submits the market job once Flink is ready. After processor code changes, follow the update steps in `processor/README.md` before rebuilding. Redpanda Console is at <http://localhost:8080/overview>, Flink at <http://localhost:8081/>, Prometheus at <http://localhost:9090/targets>, and Grafana at <http://localhost:3000/> (local login: `admin` / `admin`). The demo scenario Swagger page is at <http://localhost:8002/docs>.

Check whether the topic exists:

```powershell
docker exec eve-redpanda rpk topic list
```

Compose creates missing topics. To create one manually when running without the `topics` service:

```powershell
docker exec eve-redpanda rpk topic create market-observations -p 3 -r 1
docker exec eve-redpanda rpk topic create market.alerts -p 1 -r 1
```

Skip the create command for a topic that already exists. See [Redpanda](redpanda.md) for connection addresses and message checks.

## 3. Check the Flink job

List running jobs first:

```powershell
docker compose -f infra/compose.yml exec jobmanager flink list
```

Compose submits the market job automatically. If you started only the Flink services, submit it manually once:

```powershell
docker compose -f infra/compose.yml exec jobmanager flink run -d -py /opt/processor/run_job.py
```

Check the job in the Flink Web UI. To follow its output in a separate terminal:

```powershell
docker compose -f infra/compose.yml logs -f taskmanager
```

The job writes qualifying price-drop alerts to `market.alerts`. Inspect them in Redpanda Console or with `docker exec eve-redpanda rpk topic consume market.alerts -n 1 -o start`. The default minimum is ten prior observations for the same region and item, and the incoming price must be at least 30% below their rolling average. Set `PROCESSOR_DROP_THRESHOLD_PERCENT` before starting Compose to change the threshold.

To generate a known alert after the V2 job is running, run `.\processor\tools\send_alert_example.ps1` from the repository root. It publishes ten 100 ISK observations and one 75 ISK observation for a fresh item ID, then prints that ID. Look it up in Redpanda Console. A normal single producer snapshot may not contain enough prior observations for an alert.

Submitting the manual command again starts another job. See [Stream processor](processor.md#deploy-an-update) for deploying an update with a savepoint, and [Monitoring](monitoring.md) for the operational dashboard.

For a repeatable 100-event alert check or a configurable load run, use the [demo scenario service](demo.md). Trigger a scenario in Swagger, then poll `GET /runs/{run_id}` until it passes or fails.

## 4. Run the producer

In a new PowerShell terminal, from the repository root:

```powershell
Set-Location producer
poetry install
poetry run eve-producer --once --max-events 50
```

This fetches up to 50 observations from the mock API and publishes them to Redpanda, then exits. To keep polling, run `poetry run eve-producer --rate 20` from `producer/` and stop it with Ctrl+C. The packaged defaults and `.env.example` point to `localhost:8000` for the API and `localhost:19092` for Redpanda.

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
