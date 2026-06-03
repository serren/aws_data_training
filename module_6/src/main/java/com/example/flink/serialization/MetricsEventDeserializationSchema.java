package com.example.flink.serialization;

import com.example.flink.model.MetricsEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

public class MetricsEventDeserializationSchema implements KinesisDeserializationSchema<MetricsEvent> {

    private static final Logger log = LoggerFactory.getLogger(MetricsEventDeserializationSchema.class);

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

        log.debug("Deserialized event: component={} metric={} value=[{},{}] timestamp={}",
                event.getComponentName(), event.getMetricName(),
                event.getMinValue(), event.getMaxValue(), event.getFromTimestampMs());
        return event;
    }

    @Override
    public TypeInformation<MetricsEvent> getProducedType() {
        return TypeInformation.of(MetricsEvent.class);
    }
}
