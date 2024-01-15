package com.camacuchi.kafka.valley.domain.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record SpeedLimiter(
        String id,
        @JsonProperty("serial_number") String serialNumber,
        @JsonProperty("model_type") String modelType,
        @JsonProperty("vendor_id") String vendorId


) {
}
