package com.example.flink.config;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class KinesisSourceConfig {

    String streamName;
    @Builder.Default
    String region = "us-east-1";
    String endpointUrl;
    String accessKey;
    String secretKey;
}
