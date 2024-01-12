package com.camacuchi.kafka.valley.domain.models;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedTrackingData{
    private String limiterId;
    private String limiterSerialNumber;
    private String limiterModelType;
    private String vendorId;
    private String vendorName;
    private String vendorPhone;
}
