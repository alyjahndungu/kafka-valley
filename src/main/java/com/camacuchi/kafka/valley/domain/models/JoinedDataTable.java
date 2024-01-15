package com.camacuchi.kafka.valley.domain.models;

import lombok.Builder;

@Builder
public record JoinedDataTable(SpeedLimiterModel speedLimiter, VendorModel vendor) {
}
