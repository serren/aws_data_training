package com.example.flink;

import com.example.flink.model.MetricsEvent;
import com.example.flink.model.WindowedMetrics;
import com.example.flink.operator.MetricsAggregateFunction;
import com.example.flink.operator.MetricsWindowFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class MetricsPipeline {

    public static DataStream<WindowedMetrics> apply(DataStream<MetricsEvent> source) {
        return source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<MetricsEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, ts) -> event.getFromTimestampMs())
                                .withIdleness(Duration.ofSeconds(5))
                )
                .keyBy(e -> e.getComponentName() + "|" + e.getMetricName())
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new MetricsAggregateFunction(), new MetricsWindowFunction());
    }
}
