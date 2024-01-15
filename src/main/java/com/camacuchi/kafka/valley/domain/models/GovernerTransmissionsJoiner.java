package com.camacuchi.kafka.valley.domain.models;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class GovernerTransmissionsJoiner implements ValueJoiner<Transmissions, EnrichedLimiterVendor, EnrichedTrackingData> {
    public EnrichedTrackingData apply(Transmissions transmissions, EnrichedLimiterVendor enrichedLimiterVendor) {
        return EnrichedTrackingData
                .builder()
                .transmission(transmissions)
                .limiterVendor(enrichedLimiterVendor)
                .build();
    }
}