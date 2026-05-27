package com.example.flink.model;

import lombok.Data;

@Data
public class WindowedMetrics {

    private String componentName;
    private String metricName;
    private String windowStart;
    private String windowEnd;
    private double minValue;
    private double maxValue;
    private String unit;
    private long count;

    @Override
    public String toString() {
        return String.format(
            "WindowedMetrics{component='%s', metric='%s', window=[%s -> %s], min=%.4f, max=%.4f, unit='%s', count=%d}",
            componentName, metricName, windowStart, windowEnd, minValue, maxValue, unit, count
        );
    }
}
