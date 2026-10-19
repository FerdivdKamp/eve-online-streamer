# Overview

EVE Online mocked market API

Producer | reads from API and generates streaming data
RedPanda | Picks up the data for processing
Flink |
Grafana | Simple dashboarding of the process



### Stream processor technology decision

**Decision:** Implement the market stream processor using PyFlink.

**Rationale:** The processing workload is relatively lightweight, and Python provides consistency with the producer, consumer, and analysis components. Development speed, readability, and maintainability take priority over maximum throughput.

**Trade-off:** Python operators may introduce additional serialization and cross-language execution overhead compared with JVM-native Flink operators.

**Future optimization:** Benchmark throughput, processing latency, CPU utilization, and backpressure under increasing loads. Consider Flink SQL, JVM-native operators, or a Java implementation if profiling identifies Python execution as a significant bottleneck.

**Performance target:** Generate alerts within five seconds of an observation being published to Redpanda, under the supported workload.