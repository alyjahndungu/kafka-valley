package com.camacuchi.kafka.valley.domain.models;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record EnrichedLimiterVendor(
        String limiterId,
         String limiterSerialNumber,
         String vendorId,
         String vendorName,
         String vendorPhone) {
}
