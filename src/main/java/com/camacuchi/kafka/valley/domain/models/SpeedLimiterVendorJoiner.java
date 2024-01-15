package com.camacuchi.kafka.valley.domain.models;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class SpeedLimiterVendorJoiner implements ValueJoiner<SpeedLimiterModel, VendorModel, EnrichedLimiterVendor> {
    public EnrichedLimiterVendor apply(SpeedLimiterModel limiter, VendorModel vendor) {
        return EnrichedLimiterVendor
                .builder()
                .limiterId(limiter.speedLimiter().id())
                .limiterSerialNumber(limiter.speedLimiter().serialNumber())
                .vendorId(vendor.vendors().id())
                .vendorName(vendor.vendors().name())
                .vendorPhone(vendor.vendors().phone())
                .build();
    }
}