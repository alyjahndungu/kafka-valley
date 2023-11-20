package com.camacuchi.kafka.valley.producers;

import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
    public class TransmissionSerializer implements Serializer<Transmissions> {

        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, Transmissions data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing Transmissions", e);
            }
        }

}
