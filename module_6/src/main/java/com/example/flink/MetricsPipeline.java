package com.example.flink;

import com.example.flink.model.MetricsEvent;
import com.example.flink.model.WindowedMetrics;
import com.example.flink.operator.MetricsAggregateFunction;
import com.example.flink.operator.MetricsWindowFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class MetricsPipeline {

    private static final Logger log = LoggerFactory.getLogger(MetricsPipeline.class);

    public static DataStream<WindowedMetrics> apply(DataStream<MetricsEvent> source) {
        log.debug("Building metrics pipeline: 5-min tumbling windows, 30s out-of-orderness, 5s idleness");
        return source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<MetricsEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, ts) -> event.getFromTimestampMs())
                                .withIdleness(Duration.ofSeconds(5))
                )
                .keyBy(e -> e.getComponentName() + "|" + e.getMetricName())
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new MetricsAggregateFunction(), new MetricsWindowFunction())
                .name("5-min Tumbling Window Aggregation");
    }
}
