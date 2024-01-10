package com.camacuchi.kafka.valley.domain.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Operators(
        String id,
        String name,
        @JsonProperty("mobile_number") String mobileNumber
) {
}
