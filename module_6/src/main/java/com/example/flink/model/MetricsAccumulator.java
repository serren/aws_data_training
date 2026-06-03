package com.example.flink.model;

public class MetricsAccumulator {

    public double minValue = Double.MAX_VALUE;
    public double maxValue = -Double.MAX_VALUE;
    public long count = 0;
    public String unit = "";

}
