package com.camacuchi.kafka.valley.domain.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SpeedLimiter(
        String id,
        @JsonProperty("serial_number") String serialNumber,
        @JsonProperty("model_type") String modelType
) {
}
