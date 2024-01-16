package com.camacuchi.kafka.valley.domain.models;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;
import lombok.Builder;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record EnrichedTrackingSummary(
        String vehicleRegistrationNumber,
        String speedLimiterKeyId,
        String imei,
        String latitude,
        String latitudeDirection,
        String longitude,
        String longitudeDirection,
        String speed,
        String timestamp,
        String signal,
        String power,
        String vendorId,
        String vendorName,
        String vendorPhone,
        String operatorId,
        String operatorName,
        String operatorPhone) {
}
