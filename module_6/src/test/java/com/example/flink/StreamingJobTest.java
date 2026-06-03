package com.example.flink;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.Record;
import com.example.flink.config.KinesisSinkConfig;
import com.example.flink.config.KinesisSourceConfig;
import com.example.flink.model.MetricsEvent;
import com.example.flink.model.WindowedMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class StreamingJobTest {

    private static final Logger log = LoggerFactory.getLogger(StreamingJobTest.class);

    private static final String OUTPUT_STREAM_NAME = "test-metrics-output";

    @RegisterExtension
    static final MiniClusterExtension FLINK_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(20)
                    .setNumberTaskManagers(1)
                    .build()
    );

    private static final DockerImageName LOCAL_STACK_IMAGE =
            DockerImageName.parse("localstack/localstack:0.11.3");

    @Container
    private static final LocalStackContainer LOCAL_STACK =
            new LocalStackContainer(LOCAL_STACK_IMAGE)
                    .withServices(LocalStackContainer.Service.KINESIS);

    private static MetricsKinesisManager kinesisManager;
    private static ObjectMapper objectMapper;

    @BeforeAll
    static void setUp() throws Exception {
        System.setProperty("com.amazonaws.sdk.disableCbor", "true");
        System.setProperty("org.apache.flink.kinesis.shaded.com.amazonaws.sdk.disableCbor", "true");

        objectMapper = new ObjectMapper();
        kinesisManager = initKinesis();
        createOutputStream();
    }

    private static MetricsKinesisManager initKinesis() {
        var kinesis = AmazonKinesisClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        LOCAL_STACK.getEndpointOverride(LocalStackContainer.Service.KINESIS).toString(),
                        LOCAL_STACK.getRegion()
                ))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        LOCAL_STACK.getAccessKey(),
                        LOCAL_STACK.getSecretKey()
                )))
                .build();

        return new MetricsKinesisManager("test-metrics", kinesis, objectMapper);
    }

    private static void createOutputStream() throws Exception {
        var kinesis = kinesisManager.getKinesis();
        kinesis.createStream(OUTPUT_STREAM_NAME, 1);
        var streamInfo = kinesis.describeStream(OUTPUT_STREAM_NAME);
        while (streamInfo.getStreamDescription().getStreamStatus().equalsIgnoreCase("creating")) {
            Thread.sleep(100);
            streamInfo = kinesis.describeStream(OUTPUT_STREAM_NAME);
        }
    }

    @Test
    void shouldWriteWindowedMetricsToOutputStream(
            @InjectClusterClient ClusterClient<String> clusterClient,
            @TempDir Path tempDir) throws Exception {

        // Events spanning two 5-minute windows: [12:35 - 12:40) and [12:40 - 12:45).
        kinesisManager.publishEvents(
                MetricsEvent.of("user-service", "2020-09-01T12:35:00Z", 15.0, "cpu", 5.0, "2020-09-01T12:35:10Z", "percent"),
                MetricsEvent.of("user-service", "2020-09-01T12:36:00Z", 18.0, "cpu", 6.0, "2020-09-01T12:36:10Z", "percent"),
                MetricsEvent.of("user-service", "2020-09-01T12:37:00Z", 20.0, "cpu", 7.0, "2020-09-01T12:37:10Z", "percent"),
                MetricsEvent.of("user-service", "2020-09-01T12:38:00Z", 16.0, "cpu", 4.0, "2020-09-01T12:38:10Z", "percent"),
                MetricsEvent.of("user-service", "2020-09-01T12:39:00Z", 22.0, "cpu", 8.0, "2020-09-01T12:39:10Z", "percent"),
                // triggers window 1, populates window 2
                MetricsEvent.of("user-service", "2020-09-01T12:41:30Z", 25.0, "cpu", 10.0, "2020-09-01T12:41:40Z", "percent"),
                MetricsEvent.of("user-service", "2020-09-01T12:42:00Z", 24.0, "cpu", 9.0, "2020-09-01T12:42:10Z", "percent"),
                MetricsEvent.of("user-service", "2020-09-01T12:43:00Z", 26.0, "cpu", 11.0, "2020-09-01T12:43:10Z", "percent"),
                // triggers window 2
                MetricsEvent.of("user-service", "2020-09-01T12:46:30Z", 12.0, "cpu", 5.0, "2020-09-01T12:46:40Z", "percent")
        );

        String endpointUrl = LOCAL_STACK.getEndpointOverride(LocalStackContainer.Service.KINESIS).toString();
        String region = LOCAL_STACK.getRegion();
        String accessKey = LOCAL_STACK.getAccessKey();
        String secretKey = LOCAL_STACK.getSecretKey();

        var sourceConfig = KinesisSourceConfig.builder()
                .streamName(kinesisManager.getStreamName())
                .region(region)
                .endpointUrl(endpointUrl)
                .accessKey(accessKey)
                .secretKey(secretKey)
                .build();

        var sinkConfig = KinesisSinkConfig.builder()
                .streamName(OUTPUT_STREAM_NAME)
                .region(region)
                .endpointUrl(endpointUrl)
                .accessKey(accessKey)
                .secretKey(secretKey)
                .build();

        JobID jobId = new StreamApplication(sourceConfig, sinkConfig).run();

        // Wait for the Kinesis consumer to read all records, idleness to advance the watermark
        // (5 s idleness timeout), both windows to close, and KPL to flush to LocalStack.
        Thread.sleep(20000);

        var stopRequest = clusterClient.stopWithSavepoint(
                jobId, true, tempDir.toAbsolutePath().toString(),
                SavepointFormatType.CANONICAL
        );
        try {
            stopRequest.get(20, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof FlinkJobNotFoundException) {
                log.warn("Job {} has already completed.", jobId);
            } else {
                throw e;
            }
        }

        List<WindowedMetrics> results = readOutputRecords();

        assertFalse(results.isEmpty(), "Expected at least one WindowedMetrics record in output stream");
        assertTrue(results.size() >= 2, "Expected at least 2 WindowedMetrics records (two windows)");
        for (WindowedMetrics m : results) {
            assertTrue(m.getMinValue() <= m.getMaxValue(),
                    "minValue must be <= maxValue for " + m);
        }
        log.info("Output stream contained {} WindowedMetrics records", results.size());
        results.forEach(m -> log.info("  {}", m));
    }

    private List<WindowedMetrics> readOutputRecords() throws Exception {
        var kinesis = kinesisManager.getKinesis();
        String shardIterator = kinesis.getShardIterator(
                new GetShardIteratorRequest()
                        .withStreamName(OUTPUT_STREAM_NAME)
                        .withShardId("shardId-000000000000")
                        .withShardIteratorType("TRIM_HORIZON")
        ).getShardIterator();

        List<WindowedMetrics> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            var response = kinesis.getRecords(
                    new GetRecordsRequest().withShardIterator(shardIterator).withLimit(100));
            for (Record record : response.getRecords()) {
                WindowedMetrics metrics = objectMapper.readValue(
                        record.getData().array(), WindowedMetrics.class);
                results.add(metrics);
            }
            shardIterator = response.getNextShardIterator();
            if (!results.isEmpty()) break;
            Thread.sleep(500);
        }
        return results;
    }
}
