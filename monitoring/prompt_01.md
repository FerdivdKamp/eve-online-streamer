# Implement Prometheus and Grafana monitoring

## Context

The EVE Online market streaming project runs locally using Docker Compose.

Current components include:

- Mock API and Python producer.
- Redpanda as the Kafka-compatible message broker.
- Apache Flink 2.2.1 with JobManager and TaskManager.
- Python stream-processing logic using PyFlink.
- A rolling-average implementation and market alert topic, either already implemented or in development.

We want observability for the streaming platform without unnecessarily coupling monitoring to the business logic.

Inspect the repository and existing Docker Compose configuration before making changes.

## 1. Directory structure

Create a dedicated `monitoring/` directory:

```text
monitoring/
├── prometheus/
│   └── prometheus.yml
└── grafana/
    ├── provisioning/
    │   ├── datasources/
    │   │   └── prometheus.yml
    │   └── dashboards/
    │       └── dashboards.yml
    └── dashboards/
        └── pipeline-overview.json
```

Preserve the existing `dashboard/` directory for the EVE market product UI.

## 2. Docker Compose integration

Add Prometheus and Grafana to the existing Compose environment.

Requirements:

- Use stable, pinned image versions.
- Use the existing Docker network.
- Use Docker service names for internal connections, not localhost.
- Expose Prometheus on localhost port 9090.
- Expose Grafana on localhost port 3000.
- Persist Prometheus and Grafana data using Docker volumes.
- Configure reasonable local development retention, initially seven days.
- Configure appropriate health checks where practical.
- Avoid making Redpanda or Flink startup dependent on Grafana or Prometheus.
- Keep nonessential monitoring ports bound to localhost rather than all host interfaces.

Use an optional Docker Compose monitoring profile if compatible with the project's existing start commands.

## 3. Redpanda metrics

Configure Prometheus to scrape the Redpanda broker's existing Prometheus-compatible metrics endpoint.

Use the supported metrics endpoint for the installed Redpanda version, preferably `/public_metrics` where available.

Do not introduce a separate exporter unless required.

Expose relevant metrics for:

- Broker availability.
- Incoming and outgoing message throughput.
- Kafka consumer group lag, if exposed.
- Storage utilization.
- Broker errors and resource health.

Verify actual metric names exposed by the installed version before building dashboard queries.

## 4. Apache Flink metrics

Enable the native Flink Prometheus metrics reporter.

Requirements:

- Verify that the custom Flink image includes the appropriate Prometheus reporter JAR.
- Use the documented Flink 2.2.1 reporter configuration.
- Configure a known metrics port, initially 9249.
- Configure the reporter for both JobManager and TaskManager.
- Expose metrics to Prometheus through the Docker network.
- Ensure each Flink process can be scraped independently.
- Do not confuse Flink's Web UI port 8081 with its Prometheus metrics port.

Capture metrics for:

- JobManager and TaskManager availability.
- Job and task status.
- Records received and emitted.
- Processing throughput.
- Busy time and backpressure.
- Checkpoint duration and failures.
- Available and used resources.
- Relevant JVM memory and garbage collection metrics.

Where PyFlink custom metrics are supported, add a small number of application-level metrics for processed events, invalid events and generated alerts.

Ensure metric registration is compatible with the actual PyFlink API.

Do not rewrite the rolling-average implementation as part of this task.

## 5. Grafana configuration

Provision Prometheus as a Grafana data source automatically through version-controlled YAML.

Provision an initial dashboard named **EVE Streaming Pipeline Overview**.

Include panels for:

1. Redpanda broker availability.
2. Redpanda throughput.
3. Consumer lag, where measurable.
4. Flink JobManager and TaskManager availability.
5. Flink incoming and outgoing records per second.
6. Flink backpressure or busy time.
7. Flink checkpoint health.
8. Market alerts generated, where a suitable metric exists.

Use actual metric names discovered from the running services.

If an application-level metric is not yet implemented, document it as a follow-up rather than creating a fake metric or misleading panel.

Create the dashboard as version-controlled JSON so it appears automatically in a fresh Grafana deployment.

Use dashboard variables where helpful, for example selecting a Flink job.

## 6. Scope and ownership

Treat the Mock API and producer as upstream systems that could eventually be replaced by external providers.

Do not instrument them directly in this iteration.

Monitor their observable effects through Redpanda ingestion rate, event freshness where available, and Flink input metrics.

Do not introduce Loki, OpenTelemetry, application log topics, or a new message broker in this iteration.

Application logs can continue to use Docker stdout/stderr.

## 7. Validation

After implementing the monitoring stack:

- Run Docker Compose configuration validation.
- Start the monitoring services.
- Verify Prometheus can scrape Redpanda.
- Verify Prometheus can independently scrape the Flink JobManager and TaskManager.
- Verify Grafana connects to Prometheus.
- Verify the dashboard is provisioned automatically.
- Confirm metrics change when market events are produced and consumed.
- Verify monitoring restarts do not affect the streaming application.
- Verify persistent volumes retain dashboard and metric data across ordinary restarts.

Document any unavailable metrics or unsupported configuration.

## 8. Documentation

Add `docs/monitoring.md` documenting:

- Architecture and service responsibilities.
- How to start and stop monitoring.
- Prometheus and Grafana URLs.
- Configured scrape targets.
- How to verify target health.
- How to interpret the dashboard.
- How to troubleshoot missing metrics.
- How monitoring fits the five-second processing-to-alert objective.
- Current limitations and possible future enhancements.

Keep changes focused on operational observability. Avoid unrelated refactoring.

## Deliverables

Provide a working, reproducible Prometheus and Grafana setup, integrated with the current Compose environment, with a provisioned Grafana dashboard and verified Redpanda/Flink metrics.

At the end, summarize changed files, validation results and any remaining limitations.