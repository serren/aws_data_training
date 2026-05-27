package com.example.flink;

import com.example.flink.config.KinesisSourceConfig;
import com.example.flink.model.MetricsEvent;
import com.example.flink.serialization.MetricsEventDeserializationSchema;
import org.apache.flink.api.common.JobID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class StreamApplication {

    private final KinesisSourceConfig config;

    public StreamApplication(KinesisSourceConfig config) {
        this.config = config;
    }

    public JobID run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        buildPipeline(env);
        return env.executeAsync("Metrics Window Aggregator").getJobID();
    }

    void buildPipeline(StreamExecutionEnvironment env) {
        var source = new FlinkKinesisConsumer<MetricsEvent>(
                config.getStreamName(),
                new MetricsEventDeserializationSchema(),
                buildKinesisProperties()
        );
        MetricsPipeline.apply(env.addSource(source)).print();
    }

    private Properties buildKinesisProperties() {
        var props = new Properties();
        props.setProperty(ConsumerConfigConstants.AWS_REGION, config.getRegion());
        props.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON");

        if (config.getEndpointUrl() != null) {
            props.setProperty(ConsumerConfigConstants.AWS_ENDPOINT, config.getEndpointUrl());
            props.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
            props.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, config.getAccessKey());
            props.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, config.getSecretKey());
        }

        return props;
    }
}
