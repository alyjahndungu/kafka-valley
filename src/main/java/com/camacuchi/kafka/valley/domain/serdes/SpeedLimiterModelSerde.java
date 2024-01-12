package com.camacuchi.kafka.valley.domain.serdes;

import com.camacuchi.kafka.valley.domain.models.SpeedLimiterModel;
import com.camacuchi.kafka.valley.domain.serializers.CustomJsonDeserializer;
import com.camacuchi.kafka.valley.domain.serializers.CustomJsonSerializer;
import org.apache.kafka.common.serialization.Serdes;

public class SpeedLimiterModelSerde extends Serdes.WrapperSerde<SpeedLimiterModel> {

    public SpeedLimiterModelSerde() {
        super(new CustomJsonSerializer<>(), new CustomJsonDeserializer<>(SpeedLimiterModel.class));
    }
}