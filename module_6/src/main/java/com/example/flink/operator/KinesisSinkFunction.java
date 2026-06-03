package com.example.flink.operator;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.example.flink.config.KinesisSinkConfig;
import com.example.flink.model.WindowedMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class KinesisSinkFunction extends RichSinkFunction<WindowedMetrics> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(KinesisSinkFunction.class);

    private final KinesisSinkConfig config;
    private transient AmazonKinesis kinesis;
    private transient ObjectMapper objectMapper;

    public KinesisSinkFunction(KinesisSinkConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        objectMapper = new ObjectMapper();
        if (config.getEndpointUrl() != null) {
            kinesis = AmazonKinesisClientBuilder.standard()
                    .withEndpointConfiguration(new EndpointConfiguration(
                            config.getEndpointUrl(), config.getRegion()))
                    .withCredentials(new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(config.getAccessKey(), config.getSecretKey())))
                    .build();
        } else {
            kinesis = AmazonKinesisClientBuilder.standard()
                    .withRegion(config.getRegion())
                    .build();
        }
    }

    @Override
    public void invoke(WindowedMetrics value, Context context) throws Exception {
        byte[] payload = objectMapper.writeValueAsBytes(value);
        kinesis.putRecord(new PutRecordRequest()
                .withStreamName(config.getStreamName())
                .withPartitionKey(value.getComponentName())
                .withData(ByteBuffer.wrap(payload)));
        log.debug("Sent WindowedMetrics to stream {}: component={} metric={} min={} max={}",
                config.getStreamName(), value.getComponentName(), value.getMetricName(),
                value.getMinValue(), value.getMaxValue());
    }

    @Override
    public void close() {
        if (kinesis != null) {
            kinesis.shutdown();
        }
    }
}
