package com.camacuchi.kafka.valley.domain.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Vendors(
        String id,
        @JsonProperty("name") String name,
       @JsonProperty("phone") String phone
) {
}
