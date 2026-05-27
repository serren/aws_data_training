package com.example.flink;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.AmazonKinesisException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.example.flink.model.MetricsEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class MetricsKinesisManager {

    private static final Logger log = LoggerFactory.getLogger(MetricsKinesisManager.class);

    private final String streamName;
    private final AmazonKinesis kinesis;
    private final ObjectMapper payloadMapper;

    public MetricsKinesisManager(String streamName, AmazonKinesis kinesis, ObjectMapper payloadMapper) {
        this.streamName = streamName;
        this.kinesis = kinesis;
        this.payloadMapper = payloadMapper;

        createStream();
    }

    public String getStreamName() {
        return streamName;
    }

    private void createStream() {
        try {
            kinesis.createStream(streamName, 1);
            var streamInfo = kinesis.describeStream(streamName);
            while (streamInfo.getStreamDescription().getStreamStatus().equalsIgnoreCase("creating")) {
                log.info("Stream {} is still being created", streamName);
                Thread.sleep(100);
                streamInfo = kinesis.describeStream(streamName);
            }
        } catch (AmazonKinesisException | InterruptedException e) {
            throw new RuntimeException("Failed to create stream " + streamName, e);
        }
    }

    public void publishEvents(MetricsEvent... events) {
        for (var event : events) {
            publishEvent(event);
        }
    }

    private void publishEvent(MetricsEvent event) {
        var request = new PutRecordRequest();
        request.setStreamName(streamName);
        request.setPartitionKey("1");
        request.setData(serialise(event));

        try {
            kinesis.putRecord(request);
        } catch (AmazonKinesisException e) {
            throw new RuntimeException("Failed to publish event", e);
        }
    }

    private ByteBuffer serialise(MetricsEvent event) {
        try {
            return ByteBuffer.wrap(payloadMapper.writeValueAsBytes(event));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event", e);
        }
    }
}
