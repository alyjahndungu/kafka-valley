package com.camacuchi.kafka.valley.domain.models;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class LimiterVendorJoiner implements ValueJoiner<SpeedLimiterModel, VendorModel, JoinedDataTable> {
    public JoinedDataTable apply(SpeedLimiterModel limiter, VendorModel vendor) {
        return JoinedDataTable
                .builder()
                .speedLimiter(limiter)
                .vendor(vendor)
                .build();
    }
}