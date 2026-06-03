package com.example.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.example.flink.config.KinesisSinkConfig;
import com.example.flink.config.KinesisSourceConfig;
import com.example.flink.model.MetricsEvent;
import com.example.flink.operator.KinesisSinkFunction;
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

        Properties sourceProps = appProps.getOrDefault("KinesisConsumerConfig", new Properties());
        String sourceStreamName = sourceProps.getProperty("streamName");
        if (sourceStreamName == null || sourceStreamName.isBlank()) {
            throw new IllegalStateException("KinesisConsumerConfig.streamName application property is required");
        }
        String sourceRegion = sourceProps.getProperty("region", "us-east-1");

        Properties sinkProps = appProps.getOrDefault("KinesisSinkConfig", new Properties());
        String sinkStreamName = sinkProps.getProperty("streamName");
        if (sinkStreamName == null || sinkStreamName.isBlank()) {
            throw new IllegalStateException("KinesisSinkConfig.streamName application property is required");
        }
        String sinkRegion = sinkProps.getProperty("region", "us-east-1");

        KinesisSourceConfig sourceConfig = KinesisSourceConfig.builder()
                .streamName(sourceStreamName)
                .region(sourceRegion)
                .build();

        KinesisSinkConfig sinkConfig = KinesisSinkConfig.builder()
                .streamName(sinkStreamName)
                .region(sinkRegion)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        new StreamApplication(sourceConfig, sinkConfig).buildPipeline(env);
        env.execute("Metrics Window Aggregator");
    }

    private final KinesisSourceConfig sourceConfig;
    private final KinesisSinkConfig sinkConfig;

    public StreamApplication(KinesisSourceConfig sourceConfig, KinesisSinkConfig sinkConfig) {
        this.sourceConfig = sourceConfig;
        this.sinkConfig = sinkConfig;
    }

    public JobID run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        buildPipeline(env);
        return env.executeAsync("Metrics Window Aggregator").getJobID();
    }

    void buildPipeline(StreamExecutionEnvironment env) {
        var source = new FlinkKinesisConsumer<MetricsEvent>(
                sourceConfig.getStreamName(),
                new MetricsEventDeserializationSchema(),
                buildKinesisSourceProperties()
        );

        MetricsPipeline
                .apply(env.addSource(source).name("Kinesis Source [" + sourceConfig.getStreamName() + "]"))
                .addSink(new KinesisSinkFunction(sinkConfig))
                .name("Kinesis Sink [" + sinkConfig.getStreamName() + "]");
    }

    private Properties buildKinesisSourceProperties() {
        var props = new Properties();
        props.setProperty(ConsumerConfigConstants.AWS_REGION, sourceConfig.getRegion());
        props.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON");

        if (sourceConfig.getEndpointUrl() != null) {
            props.setProperty(ConsumerConfigConstants.AWS_ENDPOINT, sourceConfig.getEndpointUrl());
            props.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
            props.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, sourceConfig.getAccessKey());
            props.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, sourceConfig.getSecretKey());
        }

        return props;
    }
}
