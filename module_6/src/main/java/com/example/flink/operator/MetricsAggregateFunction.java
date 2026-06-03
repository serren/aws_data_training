package com.example.flink.operator;

import com.example.flink.model.MetricsAccumulator;
import com.example.flink.model.MetricsEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsAggregateFunction
        implements AggregateFunction<MetricsEvent, MetricsAccumulator, MetricsAccumulator> {

    private static final Logger log = LoggerFactory.getLogger(MetricsAggregateFunction.class);

    @Override
    public MetricsAccumulator createAccumulator() {
        return new MetricsAccumulator();
    }

    @Override
    public MetricsAccumulator add(MetricsEvent event, MetricsAccumulator acc) {
        acc.minValue = Math.min(acc.minValue, event.getMinValue());
        acc.maxValue = Math.max(acc.maxValue, event.getMaxValue());
        acc.unit = event.getUnit();
        acc.count++;
        log.debug("Accumulated event: component={} metric={} value=[{},{}] count={}",
                event.getComponentName(), event.getMetricName(),
                event.getMinValue(), event.getMaxValue(), acc.count);
        return acc;
    }

    @Override
    public MetricsAccumulator getResult(MetricsAccumulator acc) {
        return acc;
    }

    @Override
    public MetricsAccumulator merge(MetricsAccumulator a, MetricsAccumulator b) {
        MetricsAccumulator result = new MetricsAccumulator();
        result.minValue = Math.min(a.minValue, b.minValue);
        result.maxValue = Math.max(a.maxValue, b.maxValue);
        result.count = a.count + b.count;
        result.unit = a.count > 0 ? a.unit : b.unit;
        return result;
    }
}
