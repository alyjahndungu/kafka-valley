package com.camacuchi.kafka.valley.domain.serdes;

import com.camacuchi.kafka.valley.domain.models.OperatorModel;
import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.camacuchi.kafka.valley.domain.models.VendorModel;
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

    public static Serde<OperatorModel> OperatorModel() {
        JsonSerializer<OperatorModel> serializer = new JsonSerializer<>();
        JsonDeserializer<OperatorModel> deserializer = new JsonDeserializer<>(OperatorModel.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<VendorModel> VendorModel() {
        JsonSerializer<VendorModel> serializer = new JsonSerializer<>();
        JsonDeserializer<VendorModel> deserializer = new JsonDeserializer<>(VendorModel.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
