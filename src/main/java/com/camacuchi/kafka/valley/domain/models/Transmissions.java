package com.camacuchi.kafka.valley.domain.models;

import jakarta.annotation.Nullable;

public record Transmissions(
        @Nullable String imei,
         @Nullable String latitude,
       @Nullable  String longitude,
        @Nullable String name,
        @Nullable String power,
        @Nullable String signal,
        @Nullable  Integer speed,
         @Nullable String timestamp

     ) {
    }

