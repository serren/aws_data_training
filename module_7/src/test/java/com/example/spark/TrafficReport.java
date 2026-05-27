package com.example.spark;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TrafficReport {

    @JsonProperty("id")
    private String id;

    @JsonProperty("source")
    private String source;

    @JsonProperty("target")
    private String target;

    @JsonProperty("totalRequests")
    private long totalRequests;

    public TrafficReport() {}

    public TrafficReport(String id, String source, String target, long totalRequests) {
        this.id = id;
        this.source = source;
        this.target = target;
        this.totalRequests = totalRequests;
    }

    public String getId() { return id; }
    public String getSource() { return source; }
    public String getTarget() { return target; }
    public long getTotalRequests() { return totalRequests; }
}
