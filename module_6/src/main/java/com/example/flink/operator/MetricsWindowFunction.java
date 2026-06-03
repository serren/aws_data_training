package com.example.flink.operator;

import com.example.flink.model.MetricsAccumulator;
import com.example.flink.model.WindowedMetrics;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class MetricsWindowFunction
        extends ProcessWindowFunction<MetricsAccumulator, WindowedMetrics, String, TimeWindow> {

    private static final Logger log = LoggerFactory.getLogger(MetricsWindowFunction.class);

    @Override
    public void process(String key, Context context, Iterable<MetricsAccumulator> elements,
                        Collector<WindowedMetrics> out) {
        MetricsAccumulator acc = elements.iterator().next();

        String[] keyParts = key.split("\\|", 2);
        String componentName = keyParts[0];
        String metricName = keyParts.length > 1 ? keyParts[1] : "";

        TimeWindow window = context.window();

        WindowedMetrics result = new WindowedMetrics();
        result.setComponentName(componentName);
        result.setMetricName(metricName);
        result.setFromtimestamp(Instant.ofEpochMilli(window.getStart()).toString());
        result.setTotimestamp(Instant.ofEpochMilli(window.getEnd()).toString());
        result.setMinValue(acc.minValue);
        result.setMaxValue(acc.maxValue);
        result.setUnit(acc.unit);
        result.setCount(acc.count);

        log.debug("Window closed: component={} metric={} window=[{} -> {}] min={} max={} count={}",
                componentName, metricName,
                result.getFromtimestamp(), result.getTotimestamp(),
                acc.minValue, acc.maxValue, acc.count);

        out.collect(result);
    }
}
