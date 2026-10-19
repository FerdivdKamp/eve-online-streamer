# Processor Python Reference

The [stream processor guide](../processor.md) explains how the Flink job runs and how to deploy it. This page documents the Python classes and functions that implement it.

## Configuration and event contract

::: eve_processor.config.ProcessorConfig

::: eve_processor.models.MarketObservation

## Parsing and rolling calculation

::: eve_processor.processing.parse_observation

::: eve_processor.processing.classify

::: eve_processor.rolling.RollingState

::: eve_processor.rolling.evaluate

::: eve_processor.rolling.rolling_average

::: eve_processor.rolling.percentage_deviation

## PyFlink job

::: eve_processor.job.RollingAlertOperator

::: eve_processor.job.build_source

::: eve_processor.job.build_alert_sink

::: eve_processor.job.build_job
