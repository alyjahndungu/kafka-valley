package com.camacuchi.kafka.valley.domain.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum EValleyTopics {

    TOPIC_TRANSMISSIONS("transmissions"),
    TOPIC_OVER_SPEEDING("over_speeding");

    private final  String name;

    }
