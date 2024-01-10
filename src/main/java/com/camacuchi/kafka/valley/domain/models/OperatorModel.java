package com.camacuchi.kafka.valley.domain.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record OperatorModel(@JsonProperty("after") Operators operators) {
    public static record Operators(
            String id,
            String name,
            @JsonProperty("mobile_number") String mobileNumber
    ) {
    }
}