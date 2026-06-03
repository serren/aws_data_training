package com.example.flink.operator;

import com.example.flink.model.WindowedMetrics;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingWindowedMetricsSink implements SinkFunction<WindowedMetrics> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LoggingWindowedMetricsSink.class);

    @Override
    public void invoke(WindowedMetrics value, Context context) {
        LOG.info("{}", value);
    }
}
