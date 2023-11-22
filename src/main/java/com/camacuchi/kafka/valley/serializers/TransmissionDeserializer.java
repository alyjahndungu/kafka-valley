package com.camacuchi.kafka.valley.serializers;

import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class TransmissionDeserializer implements Deserializer<Transmissions> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Transmissions deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(new String(data), Transmissions.class);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

}