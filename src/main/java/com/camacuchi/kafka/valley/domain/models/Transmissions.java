package com.camacuchi.kafka.valley.domain.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;


@JsonIgnoreProperties(ignoreUnknown = true)
public record Transmissions  (
        @Nullable @JsonProperty("name") String name,
        @Nullable  @JsonProperty("imei") String imei,
        @Nullable @JsonProperty("latitude") String latitude,
        @Nullable @JsonProperty("longitude") String longitude,
        @Nullable @JsonProperty("speed") String speed,
        @Nullable @JsonProperty("timestamp") String timestamp,
        @Nullable @JsonProperty("signal") String signal,
        @Nullable @JsonProperty("power") String power){
}