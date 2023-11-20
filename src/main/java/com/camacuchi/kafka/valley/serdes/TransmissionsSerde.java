package com.camacuchi.kafka.valley.serdes;

import com.camacuchi.kafka.valley.domain.models.Transmissions;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class TransmissionsSerde extends Serdes.WrapperSerde<Transmissions> {
    public TransmissionsSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(Transmissions.class));
    }
}