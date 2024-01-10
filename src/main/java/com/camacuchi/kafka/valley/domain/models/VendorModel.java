package com.camacuchi.kafka.valley.domain.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record VendorModel(@JsonProperty("after") Vendors vendors) {
    public static record Vendors(
            String id,
            String name,
            String phone
    ) {
    }
}