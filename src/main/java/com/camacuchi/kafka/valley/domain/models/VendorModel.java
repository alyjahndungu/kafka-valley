package com.camacuchi.kafka.valley.domain.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record VendorModel(@JsonProperty("after") Vendors vendors) {

}

