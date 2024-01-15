package com.camacuchi.kafka.valley.domain.models;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class LimiterVendorJoiner implements ValueJoiner<SpeedLimiter, Vendors, JoinedDataTable> {
    public JoinedDataTable apply(SpeedLimiter limiter, Vendors vendor) {
        return JoinedDataTable
                .builder()
                .speedLimiter(limiter)
                .vendor(vendor)
                .build();
    }
}