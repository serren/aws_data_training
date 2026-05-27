package com.example.flink.model;

import lombok.Data;

import java.time.Instant;

@Data
public class MetricsEvent {

    private String componentName;
    private long fromTimestampMs;
    private double maxValue;
    private String metricName;
    private double minValue;
    private long toTimestampMs;
    private String unit;

    public static MetricsEvent of(String componentName, String fromTimestamp, double maxValue,
                                   String metricName, double minValue, String toTimestamp, String unit) {
        MetricsEvent e = new MetricsEvent();
        e.setComponentName(componentName);
        e.setFromTimestampMs(Instant.parse(fromTimestamp).toEpochMilli());
        e.setMaxValue(maxValue);
        e.setMetricName(metricName);
        e.setMinValue(minValue);
        e.setToTimestampMs(Instant.parse(toTimestamp).toEpochMilli());
        e.setUnit(unit);
        return e;
    }

    public static MetricsEvent fromCsv(String line) {
        String[] parts = line.split(",");
        MetricsEvent event = new MetricsEvent();
        event.componentName = parts[0].trim();
        event.fromTimestampMs = Instant.parse(parts[1].trim()).toEpochMilli();
        event.maxValue = Double.parseDouble(parts[2].trim());
        event.metricName = parts[3].trim();
        event.minValue = Double.parseDouble(parts[4].trim());
        event.toTimestampMs = Instant.parse(parts[5].trim()).toEpochMilli();
        event.unit = parts[6].trim();
        return event;
    }
}
