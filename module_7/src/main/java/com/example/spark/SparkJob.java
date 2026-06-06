package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.spark.sql.functions.*;

public class SparkJob {

    // Group 3 = source service name, Group 4 = ISO8601 timestamp (without brackets)
    private static final String LOG_REGEX =
            "^(\\S+)\\s+(-+)\\s+(\\S+)\\s+\"[^\"]+\"\\s+\\d+\\s+\\[([^\\]]+)]\\s+\"[^\"]+\"$";

    // Extracts the service name prefix from a file path like ".../audit-service-2022-09-15.txt"
    private static final String FILENAME_REGEX =
            "([^/]+)-\\d{4}-\\d{2}-\\d{2}\\.txt$";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: SparkJob <inputPath> <outputPath> [hoursBack]");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];
        int hoursBack = args.length > 2 ? Integer.parseInt(args[2]) : 24;

        SparkSession spark = SparkSession.builder()
                .appName("ServerAccessLogAnalytics")
                .getOrCreate();

        run(spark, inputPath, outputPath, hoursBack, Instant.now());
        spark.stop();
    }

    public static void run(SparkSession spark, String inputPath, String outputPath,
                           int hoursBack, Instant referenceTime) {
        long cutoffEpochSeconds = referenceTime.minus(hoursBack, ChronoUnit.HOURS).getEpochSecond();

        Dataset<Row> raw = spark.read().text(inputPath + "/*");

        Dataset<Row> result = raw
                .withColumn("file_path", input_file_name())
                .withColumn("target", regexp_extract(col("file_path"), FILENAME_REGEX, 1))
                .withColumn("caller", regexp_extract(col("value"), LOG_REGEX, 1))
                .withColumn("source", regexp_extract(col("value"), LOG_REGEX, 3))
                .withColumn("ts_str", regexp_extract(col("value"), LOG_REGEX, 4))
                .filter(col("caller").equalTo("<masked>"))
                .filter(col("source").notEqual(""))
                .withColumn("ts", coalesce(
                        to_timestamp(col("ts_str"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                        to_timestamp(col("ts_str"), "yyyy-MM-dd'T'HH:mm:ss'Z'")))
                .filter(col("ts").isNotNull())
                .filter(unix_timestamp(col("ts")).geq(cutoffEpochSeconds))
                .groupBy(col("source"), col("target"))
                .agg(count("*").as("totalRequests"))
                .withColumn("id", substring(md5(concat(col("source"), col("target"))), 1, 16))
                .select(col("id"), col("source"), col("target"), col("totalRequests"));

        result.write()
                .mode(SaveMode.Overwrite)
                .json(outputPath + "/reports");
    }
}
