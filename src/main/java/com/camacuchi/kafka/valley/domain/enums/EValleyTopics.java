package com.camacuchi.kafka.valley.domain.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum EValleyTopics {

    TOPIC_TRANSMISSIONS("transmissions"),
    TOPIC_LIMITERS("tracking.staging.speed_limiters"),
    TOPIC_OVER_SPEEDING("over_speeding"),
    TOPIC_VENDORS("dbz_.staging.vendors"),
    TOPIC_OPERATORS("dbz_.staging.operators"),
    TOPIC_JOIN_EVENTS("joined_events"),
    TOPIC_ENRICHED_TRACKER_RESULT("ENRICHED-TRACKER-RESULT"),
    TOPIC_ENRICHED_TRANSMISSIONS("ENRICHED-TRANSMISSIONS"),
    TOPIC_SPEED_LIMITERS("dbz_.staging.speed_limiters");

    private final  String name;

    }
