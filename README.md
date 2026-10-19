# eve Online Market Price Notifier

## Introduction
Project is a case for a job application.

The goal is to have the plan and start for a robust streaming data pipeline that serves the purpose of signalling when an event matches specific conditions. 

The condition in question is a 30% dip in market price for an item against a 7-day rolling average.


## Deliverables Criteria

* high-level design of an application that enables data to be processed in a streaming fashion
* show how proficient you are with applying software engineering best-practices while implementing this application
* essential that you can communicate results and convince the client that high-quality work has been delivered
* short presentation of your solution, convince both engineering peers and oblivious clients of the quality of your solution

## Requirements
Must have MVP
* Python
* Process data in streaming fashion

Should have (production ready)
* Horizontally scalable in copute
* Show real-time results (latency max 5 seconds)
* Appropriate unittests

Could have
* Possible to implement the solution in the cloud
* Solution should be horizontally scalable in terms of data storage capacity. Assume not all data can fit in one computer’s memory and/or disk. Hint: think of how you are partitioning the data (if that’s applicable to your use case).


### Setting up
Start Docker Desktop, then run `docker compose -f infra/compose.yml up -d --build` from the repository root. This starts the mock API, demo scenario service, Redpanda, Flink processor, Prometheus, and Grafana. See the [quickstart](docs/quickstart.md) for the producer and demo steps. The mock API's Swagger UI is at <http://localhost:8000/>; the scenario controls are at <http://localhost:8002/docs>.

If Docker cannot pull the mock API image from GitHub Container Registry, authenticate first:

```powershell
$env:CR_PAT = "<YOUR_PERSONAL_ACCESS_TOKEN>"
$env:CR_PAT | docker login ghcr.io -u FerdivdKamp --password-stdin
```



## Redpanda

See Infra for steps to get it running.

## Documentation

poetry -C docs run mkdocs serve -f ../mkdocs.yml --dev-addr 127.0.0.1:8001

subsequently visit: http://127.0.0.1:8001/

## Continuous integration

GitHub Actions runs Ruff on the producer and consumer Python code and each package's `unittest` suite on Python 3.13. The workflow runs on pushes and pull requests. Ruff findings are advisory; unittest failures fail their jobs. To block merging into `master` when tests fail, require both `unittest (producer)` and `unittest (consumer)` in the repository's branch protection rule or ruleset. Leave `lint (advisory)` out of the required checks.

To run the same checks locally from the repository root:

```powershell
python -m pip install ruff==0.17.0
ruff check producer consumer
poetry -C producer run python -m unittest discover -s tests -v
poetry -C consumer run python -m unittest discover -s tests -v
```
