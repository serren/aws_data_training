package com.example.flink.serialization;

import com.example.flink.model.MetricsEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import java.io.IOException;
import java.time.Instant;

public class MetricsEventDeserializationSchema implements KinesisDeserializationSchema<MetricsEvent> {

    private transient ObjectMapper objectMapper;

    @Override
    public MetricsEvent deserialize(byte[] recordValue, String partitionKey, String seqNum,
                                    long approxArrivalTimestamp, String stream, String shardId)
            throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        JsonNode node = objectMapper.readTree(recordValue);

        MetricsEvent event = new MetricsEvent();
        event.setMetricName(node.path("metricName").asText());
        event.setComponentName(node.path("componentName").asText());
        event.setUnit(node.path("unit").asText());

        // generator format: single "value" field
        // integration-test format: separate "minValue" / "maxValue" fields
        if (node.has("value")) {
            double v = node.path("value").asDouble();
            event.setMinValue(v);
            event.setMaxValue(v);
        } else {
            event.setMinValue(node.path("minValue").asDouble());
            event.setMaxValue(node.path("maxValue").asDouble());
        }

        // generator format: ISO-8601 "publicationTimestamp"
        // integration-test format: epoch-ms "fromTimestampMs" / "toTimestampMs"
        if (node.has("publicationTimestamp")) {
            long epochMs = Instant.parse(node.path("publicationTimestamp").asText()).toEpochMilli();
            event.setFromTimestampMs(epochMs);
            event.setToTimestampMs(epochMs);
        } else {
            event.setFromTimestampMs(node.path("fromTimestampMs").asLong());
            event.setToTimestampMs(node.path("toTimestampMs").asLong());
        }

        return event;
    }

    @Override
    public TypeInformation<MetricsEvent> getProducedType() {
        return TypeInformation.of(MetricsEvent.class);
    }
}
