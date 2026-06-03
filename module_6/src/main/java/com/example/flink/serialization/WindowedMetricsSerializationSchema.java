package com.example.flink.serialization;

import com.example.flink.model.WindowedMetrics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema;

import java.nio.ByteBuffer;

public class WindowedMetricsSerializationSchema implements KinesisSerializationSchema<WindowedMetrics> {

    private transient ObjectMapper objectMapper;

    @Override
    public ByteBuffer serialize(WindowedMetrics element) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            return ByteBuffer.wrap(objectMapper.writeValueAsBytes(element));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize WindowedMetrics", e);
        }
    }

    @Override
    public String getTargetStream(WindowedMetrics element) {
        return null;
    }
}
