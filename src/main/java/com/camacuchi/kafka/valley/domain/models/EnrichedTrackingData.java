package com.camacuchi.kafka.valley.domain.models;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class EnrichedTrackingData{
    EnrichedLimiterVendor limiterVendor;
    Transmissions transmission;
}
