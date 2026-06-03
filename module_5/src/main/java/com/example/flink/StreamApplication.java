package com.example.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.example.flink.config.KinesisSourceConfig;
import com.example.flink.model.MetricsEvent;
import com.example.flink.operator.LoggingWindowedMetricsSink;
import com.example.flink.serialization.MetricsEventDeserializationSchema;
import org.apache.flink.api.common.JobID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Map;
import java.util.Properties;

public class StreamApplication {

    public static void main(String[] args) throws Exception {
        Map<String, Properties> appProps = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties kinesisProps = appProps.getOrDefault("KinesisConsumerConfig", new Properties());

        String streamName = kinesisProps.getProperty("streamName");
        if (streamName == null || streamName.isBlank()) {
            throw new IllegalStateException("KinesisConsumerConfig.streamName application property is required");
        }
        String region = kinesisProps.getProperty("region", "us-east-1");

        KinesisSourceConfig config = KinesisSourceConfig.builder()
                .streamName(streamName)
                .region(region)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        new StreamApplication(config).buildPipeline(env);
        env.execute("Metrics Window Aggregator");
    }

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
        MetricsPipeline.apply(env.addSource(source)).addSink(new LoggingWindowedMetricsSink());
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
