package com.camacuchi.kafka.valley.domain.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Operators(
        String id,
        String name,
        @JsonProperty("mobile_number") String mobileNumber
) {
}
