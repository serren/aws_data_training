package com.example.flink;

import com.example.flink.model.MetricsEvent;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Usage: StreamingJob <path-to-csv>");
        }
        String csvPath = args[0];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MetricsPipeline
                .apply(
                        env.readTextFile(csvPath)
                                .filter(line -> !line.startsWith("componentName"))
                                .map(MetricsEvent::fromCsv)
                )
                .print();
        env.execute("Metrics Window Aggregator");
    }
}
