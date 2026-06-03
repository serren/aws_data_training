package com.example.flink.operator;

import com.example.flink.model.MetricsAccumulator;
import com.example.flink.model.WindowedMetrics;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricsWindowFunctionTest {

    private final MetricsWindowFunction fn = new MetricsWindowFunction();

    @Test
    void process_populatesAllFieldsFromAccumulatorAndWindow() throws Exception {
        long windowStart = Instant.parse("2020-01-01T12:00:00Z").toEpochMilli();
        long windowEnd   = Instant.parse("2020-01-01T12:05:00Z").toEpochMilli();

        MetricsAccumulator acc = new MetricsAccumulator();
        acc.minValue = 5.0;
        acc.maxValue = 42.0;
        acc.count = 3;
        acc.unit = "percent";

        List<WindowedMetrics> output = new ArrayList<>();
        fn.process("user-service|cpu", testContext(windowStart, windowEnd),
                Collections.singletonList(acc), listCollector(output));

        assertEquals(1, output.size());
        WindowedMetrics result = output.get(0);

        assertEquals("user-service", result.getComponentName());
        assertEquals("cpu", result.getMetricName());
        assertEquals(5.0, result.getMinValue());
        assertEquals(42.0, result.getMaxValue());
        assertEquals("percent", result.getUnit());
        assertEquals(3, result.getCount());
        assertEquals(Instant.ofEpochMilli(windowStart).toString(), result.getFromtimestamp());
        assertEquals(Instant.ofEpochMilli(windowEnd).toString(), result.getTotimestamp());
    }

    @Test
    void process_keyWithNoPipe_usesFullKeyAsComponentName() throws Exception {
        MetricsAccumulator acc = new MetricsAccumulator();
        acc.minValue = 1.0;
        acc.maxValue = 2.0;
        acc.unit = "ms";

        List<WindowedMetrics> output = new ArrayList<>();
        fn.process("order-service", testContext(0, 1000),
                Collections.singletonList(acc), listCollector(output));

        assertEquals("order-service", output.get(0).getComponentName());
        assertEquals("", output.get(0).getMetricName());
    }

    private static Collector<WindowedMetrics> listCollector(List<WindowedMetrics> list) {
        return new Collector<WindowedMetrics>() {
            @Override public void collect(WindowedMetrics record) { list.add(record); }
            @Override public void close() {}
        };
    }

    private static ProcessWindowFunction<MetricsAccumulator, WindowedMetrics, String, TimeWindow>.Context
            testContext(long start, long end) {
        TimeWindow window = new TimeWindow(start, end);
        return new MetricsWindowFunction().new Context() {
            @Override public TimeWindow window() { return window; }
            @Override public long currentWatermark() { return window.getEnd(); }
            @Override public long currentProcessingTime() { return window.getEnd(); }
            @Override public KeyedStateStore windowState() { throw new UnsupportedOperationException(); }
            @Override public KeyedStateStore globalState() { throw new UnsupportedOperationException(); }
            @Override public <X> void output(OutputTag<X> tag, X value) { throw new UnsupportedOperationException(); }
        };
    }
}
