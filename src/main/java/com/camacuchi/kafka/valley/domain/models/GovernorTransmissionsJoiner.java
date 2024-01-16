package com.camacuchi.kafka.valley.domain.models;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class GovernorTransmissionsJoiner implements ValueJoiner<EnrichedLimiterVendor, Transmissions, EnrichedTrackingSummary> {


    public EnrichedTrackingSummary apply(EnrichedLimiterVendor enrichedLimiterVendor, Transmissions transmissions) {
        if (enrichedLimiterVendor == null || transmissions == null) {
            return null;
        }

        return EnrichedTrackingSummary
                .builder()
                .vehicleRegistrationNumber(transmissions.name())
                .speedLimiterKeyId(enrichedLimiterVendor.limiterId())
                .imei(enrichedLimiterVendor.limiterSerialNumber())
                .latitude(transmissions.latitude())
                .latitudeDirection("")
                .longitude(transmissions.longitude())
                .longitudeDirection("")
                .speed(transmissions.speed())
                .timestamp(transmissions.timestamp())
                .signal(transmissions.signal())
                .power(transmissions.power())
                .vendorId(enrichedLimiterVendor.vendorId())
                .vendorName(enrichedLimiterVendor.vendorName())
                .vendorPhone(enrichedLimiterVendor.vendorPhone())
                .operatorId("")
                .operatorName("")
                .operatorPhone("")
                .build();
    }
}