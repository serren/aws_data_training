package com.example.spark;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class SparkJobTest {

    @Container
    private static final LocalStackContainer LOCAL_STACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.4"))
                    .withServices(LocalStackContainer.Service.S3);

    private static AmazonS3 s3Client;
    private static SparkSession spark;

    private static final String INPUT_BUCKET = "test-access-logs";
    private static final String OUTPUT_BUCKET = "test-traffic-reports";

    @BeforeAll
    static void setUp() {
        s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        LOCAL_STACK.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
                        LOCAL_STACK.getRegion()
                ))
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(LOCAL_STACK.getAccessKey(), LOCAL_STACK.getSecretKey())
                ))
                .withPathStyleAccessEnabled(true)
                .build();

        s3Client.createBucket(INPUT_BUCKET);
        s3Client.createBucket(OUTPUT_BUCKET);
        uploadTestLogs();

        String endpoint = LOCAL_STACK.getEndpointOverride(LocalStackContainer.Service.S3).toString();

        spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkJobTest")
                .config("spark.hadoop.fs.s3a.endpoint", endpoint)
                .config("spark.hadoop.fs.s3a.access.key", "test")
                .config("spark.hadoop.fs.s3a.secret.key", "test")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .config("spark.hadoop.fs.s3a.connection.maximum", "5")
                .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
                .config("spark.ui.enabled", "false")
                // hadoop-client-api:3.4.3 (test scope, JDK 25 UGI fix) changed these defaults
                // to duration strings; hadoop-aws:3.3.6's Configuration.getLong() can't parse them
                .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
                .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
                .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
                .config("spark.hadoop.fs.s3a.connection.ttl", "300000")
                .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
                // Use in-memory buffer to avoid creating local temp files (winutils not available on CI)
                .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
                .getOrCreate();
    }

    private static void uploadTestLogs() {
        // referenceTime in the test = 2022-09-15T14:00:00Z
        // cutoff (24h back)         = 2022-09-14T14:00:00Z
        // Timestamps below are at 2022-09-15T13:xx — WITHIN the 24h window

        // Third order-service line has no milliseconds — verifies the no-millis timestamp fix
        String auditLogs =
                "<masked>  - order-service \"POST /api/v1/audit\" 200 [2022-09-15T13:43:01.721Z] \"Apache HTTP Client\"\n" +
                "<masked>  - order-service \"POST /api/v1/audit\" 200 [2022-09-15T13:43:16.976Z] \"Apache HTTP Client\"\n" +
                "<masked>  - inventory-service \"POST /api/v1/audit\" 200 [2022-09-15T13:43:02.561Z] \"Apache HTTP Client\"\n" +
                "<masked>  - order-service \"POST /api/v1/audit\" 200 [2022-09-15T13:44:00Z] \"Apache HTTP Client\"\n";

        // Timestamp 2022-09-14T12:00Z is BEFORE the cutoff — must be filtered out
        String userLogs =
                "<masked>  - order-service \"GET /api/v1/users\" 200 [2022-09-14T12:00:00.000Z] \"Apache HTTP Client\"\n";

        // User-originated traffic (real IP, non-service remoteUser) — must not appear in reports
        String userServiceLogs =
                "192.168.15.15  - 129837ejghdfdhg \"GET /api/v1/users/{id}\" 200 [2022-09-15T13:45:00Z] \"Chrome\"\n";

        s3Client.putObject(INPUT_BUCKET, "audit-service-2022-09-15.txt", auditLogs);
        s3Client.putObject(INPUT_BUCKET, "user-service-2022-09-14.txt", userLogs);
        s3Client.putObject(INPUT_BUCKET, "user-service-2022-09-15.txt", userServiceLogs);
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void shouldProduceCorrectTrafficReports() throws Exception {
        Instant referenceTime = Instant.parse("2022-09-15T14:00:00Z");

        SparkJob.run(spark, "s3a://" + INPUT_BUCKET, "s3a://" + OUTPUT_BUCKET, 24, referenceTime);

        List<TrafficReport> reports = readReports();

        assertEquals(2, reports.size(),
                "Expected exactly 2 traffic reports (user-originated traffic must be excluded), got: " + reports.size());

        TrafficReport orderToAudit = reports.stream()
                .filter(r -> "order-service".equals(r.getSource()) && "audit-service".equals(r.getTarget()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing order-service -> audit-service report"));
        assertEquals(3L, orderToAudit.getTotalRequests());
        assertEquals(16, orderToAudit.getId().length());

        TrafficReport inventoryToAudit = reports.stream()
                .filter(r -> "inventory-service".equals(r.getSource()) && "audit-service".equals(r.getTarget()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing inventory-service -> audit-service report"));
        assertEquals(1L, inventoryToAudit.getTotalRequests());
        assertEquals(16, inventoryToAudit.getId().length());
    }

    private List<TrafficReport> readReports() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<TrafficReport> reports = new ArrayList<>();

        List<S3ObjectSummary> summaries = s3Client
                .listObjects(OUTPUT_BUCKET, "reports/")
                .getObjectSummaries();

        for (S3ObjectSummary summary : summaries) {
            String key = summary.getKey();
            if (key.endsWith("_SUCCESS") || key.endsWith(".crc")) {
                continue;
            }
            try (InputStream is = s3Client.getObject(OUTPUT_BUCKET, key).getObjectContent()) {
                String content = new String(is.readAllBytes());
                for (String line : content.split("\n")) {
                    if (!line.isBlank()) {
                        reports.add(mapper.readValue(line.trim(), TrafficReport.class));
                    }
                }
            }
        }
        return reports;
    }
}
