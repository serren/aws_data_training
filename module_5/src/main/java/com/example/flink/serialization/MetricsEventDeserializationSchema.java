package com.example.flink.serialization;

import com.example.flink.model.MetricsEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import java.io.IOException;

public class MetricsEventDeserializationSchema implements KinesisDeserializationSchema<MetricsEvent> {

    private transient ObjectMapper objectMapper;

    @Override
    public MetricsEvent deserialize(byte[] recordValue, String partitionKey, String seqNum,
                                    long approxArrivalTimestamp, String stream, String shardId)
            throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper.readValue(recordValue, MetricsEvent.class);
    }

    @Override
    public TypeInformation<MetricsEvent> getProducedType() {
        return TypeInformation.of(MetricsEvent.class);
    }
}
