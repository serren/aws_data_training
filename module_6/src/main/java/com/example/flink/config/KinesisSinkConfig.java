package com.example.flink.config;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;

@Value
@Builder
public class KinesisSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    String streamName;
    @Builder.Default
    String region = "us-east-1";
    String endpointUrl;
    String accessKey;
    String secretKey;
}
