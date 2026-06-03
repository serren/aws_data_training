package com.example.flink.operator;

import com.example.flink.model.MetricsAccumulator;
import com.example.flink.model.MetricsEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricsAggregateFunctionTest {

    private final MetricsAggregateFunction fn = new MetricsAggregateFunction();

    @Test
    void createAccumulator_initialValues() {
        MetricsAccumulator acc = fn.createAccumulator();
        assertEquals(Double.MAX_VALUE, acc.minValue);
        assertEquals(-Double.MAX_VALUE, acc.maxValue);
        assertEquals(0, acc.count);
    }

    @Test
    void add_singleEvent_setsMinMaxAndCount() {
        MetricsAccumulator acc = fn.createAccumulator();
        acc = fn.add(event("user-service", "cpu", 10.0, 20.0, "percent"), acc);

        assertEquals(10.0, acc.minValue);
        assertEquals(20.0, acc.maxValue);
        assertEquals(1, acc.count);
        assertEquals("percent", acc.unit);
    }

    @Test
    void add_multipleEvents_tracksOverallMinMax() {
        MetricsAccumulator acc = fn.createAccumulator();
        acc = fn.add(event("user-service", "cpu", 30.0, 50.0, "percent"), acc);
        acc = fn.add(event("user-service", "cpu", 10.0, 70.0, "percent"), acc);
        acc = fn.add(event("user-service", "cpu", 20.0, 40.0, "percent"), acc);

        assertEquals(10.0, acc.minValue);
        assertEquals(70.0, acc.maxValue);
        assertEquals(3, acc.count);
    }

    @Test
    void merge_combinesTwoAccumulators() {
        MetricsAccumulator a = fn.createAccumulator();
        a = fn.add(event("user-service", "cpu", 5.0, 15.0, "percent"), a);
        a = fn.add(event("user-service", "cpu", 8.0, 12.0, "percent"), a);

        MetricsAccumulator b = fn.createAccumulator();
        b = fn.add(event("user-service", "cpu", 1.0, 99.0, "percent"), b);

        MetricsAccumulator merged = fn.merge(a, b);

        assertEquals(1.0, merged.minValue);
        assertEquals(99.0, merged.maxValue);
        assertEquals(3, merged.count);
    }

    @Test
    void getResult_returnsSameAccumulator() {
        MetricsAccumulator acc = fn.createAccumulator();
        fn.add(event("order-service", "ram", 40.0, 60.0, "percent"), acc);

        MetricsAccumulator result = fn.getResult(acc);

        assertEquals(acc, result);
    }

    private static MetricsEvent event(String component, String metric, double min, double max, String unit) {
        return MetricsEvent.of(component, "2020-01-01T12:00:00Z", max, metric, min, "2020-01-01T12:00:01Z", unit);
    }
}
