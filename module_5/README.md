# Module 5 — Apache Flink Metrics Aggregator

An Apache Flink streaming application that reads metric events, applies 5-minute tumbling windows, and outputs aggregated results per component and metric type.

## What it does

1. Reads metric events from a CSV source (columns: `componentName`, `fromTimestamp`, `maxValue`, `metricName`, `minValue`, `toTimestamp`, `unit`)
2. Assigns event time using the `fromTimestamp` field with a 30-second out-of-orderness bound and 5-second source idleness timeout
3. Keys the stream by `(componentName, metricName)`
4. Applies 5-minute tumbling event-time windows
5. Reduces each window to a `WindowedMetrics` aggregate containing:
   - `windowStart` / `windowEnd` — ISO-8601 UTC boundaries
   - `minValue` — minimum of all `minValue` fields in the window
   - `maxValue` — maximum of all `maxValue` fields in the window
   - `count` — number of events in the window
6. Prints the results to stdout

## Project structure

```
module_5/
├── pom.xml
└── src/main/java/com/example/flink/
    ├── StreamingJob.java                        # entry point
    ├── model/
    │   ├── MetricsEvent.java                    # input POJO
    │   ├── MetricsAccumulator.java              # window accumulator
    │   └── WindowedMetrics.java                 # output POJO
    └── operator/
        ├── MetricsAggregateFunction.java        # AggregateFunction
        └── MetricsWindowFunction.java           # ProcessWindowFunction
```

## Requirements

- Java 17 (corretto-17 or equivalent)
- Maven 3.6+

## Running locally

The `local` Maven profile promotes Flink dependencies to `compile` scope so they are available at runtime.
Java 17 requires `--add-opens` flags passed via `MAVEN_OPTS`.

```bash
cd module_5

MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED \
            --add-opens java.base/java.util=ALL-UNNAMED \
            --add-opens java.base/java.io=ALL-UNNAMED" \
mvn exec:java -Plocal \
  -Dexec.mainClass=com.example.flink.StreamingJob \
  -Dexec.args="../test_data_generator/test-output/user-service.csv"
```

The CSV path is a required argument — the application throws `IllegalArgumentException` if omitted.

**Expected output** (user-service.csv, ~7 hours of 10-second intervals):
```
6> WindowedMetrics{component='user-service', metric='cpu', window=[2020-09-01T12:35:00Z -> 2020-09-01T12:40:00Z], min=7.5836, max=27.7639, unit='percent', count=30}
1> WindowedMetrics{component='user-service', metric='ram', window=[2020-09-01T12:35:00Z -> 2020-09-01T12:40:00Z], min=34.2420, max=92.9230, unit='percent', count=30}
...
```
168 lines total — 84 windows × 2 metric types.

## Running integration tests

The integration test (`StreamingJobTest`) requires Docker with LocalStack (Kinesis). It publishes 9 test events spanning two 5-minute windows and verifies the Flink pipeline processes them correctly.

For Docker and WSL2 setup see [DOCKER.md](../DOCKER.md).

```bash
cd module_5

MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED \
            --add-opens java.base/java.util=ALL-UNNAMED \
            --add-opens java.base/java.io=ALL-UNNAMED" \
mvn test
```

## Building the AWS deployment JAR

```bash
mvn clean package
```

Produces `target/flink-metrics-1.0-SNAPSHOT.jar` — a shaded uber JAR with Flink runtime excluded (provided by AWS Managed Flink at runtime).
