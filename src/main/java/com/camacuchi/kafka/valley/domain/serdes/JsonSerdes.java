package com.camacuchi.kafka.valley.domain.serdes;

import com.camacuchi.kafka.valley.domain.models.*;
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

    public static Serde<SpeedLimiterModel> SpeedLimiterModel() {
        JsonSerializer<SpeedLimiterModel> serializer = new JsonSerializer<>();
        JsonDeserializer<SpeedLimiterModel> deserializer = new JsonDeserializer<>(SpeedLimiterModel.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<JoinedDataTable> JoinedDataTable() {
        JsonSerializer<JoinedDataTable> serializer = new JsonSerializer<>();
        JsonDeserializer<JoinedDataTable> deserializer = new JsonDeserializer<>(JoinedDataTable.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<SpeedLimiter> SpeedLimiter() {
        JsonSerializer<SpeedLimiter> serializer = new JsonSerializer<>();
        JsonDeserializer<SpeedLimiter> deserializer = new JsonDeserializer<>(SpeedLimiter.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<Vendors> Vendors() {
        JsonSerializer<Vendors> serializer = new JsonSerializer<>();
        JsonDeserializer<Vendors> deserializer = new JsonDeserializer<>(Vendors.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
