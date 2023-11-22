package com.camacuchi.kafka.valley.domain.serdes;

import com.camacuchi.kafka.valley.domain.models.Transmissions;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

@Component
public class JsonSerdes {
    public static Serde<Transmissions> Transmissions() {
        JsonSerializer<Transmissions> serializer = new JsonSerializer<>();
        JsonDeserializer<Transmissions> deserializer = new JsonDeserializer<>(Transmissions.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
