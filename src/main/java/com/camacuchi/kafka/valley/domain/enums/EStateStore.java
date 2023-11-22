package com.camacuchi.kafka.valley.domain.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum EStateStore {

    TRANSMISSION_COUNT_STORE("transmissions_count"),
    OVER_SPEEDING_STORE ("over_speeding");

    private final  String name;
}
