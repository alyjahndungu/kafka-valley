package com.camacuchi.kafka.valley.domain.models;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class GovernerTransmissionsJoiner implements ValueJoiner< EnrichedLimiterVendor, Transmissions, EnrichedTrackingData> {
    public EnrichedTrackingData apply(EnrichedLimiterVendor enrichedLimiterVendor, Transmissions transmissions ) {
        return EnrichedTrackingData
                .builder()
                .transmission(transmissions)
                .limiterVendor(enrichedLimiterVendor)
                .build();
    }
}