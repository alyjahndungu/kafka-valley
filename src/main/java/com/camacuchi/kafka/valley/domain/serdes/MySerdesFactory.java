package com.camacuchi.kafka.valley.domain.serdes;

import com.camacuchi.kafka.valley.domain.models.*;
import com.camacuchi.kafka.valley.domain.serializers.CustomJsonDeserializer;
import com.camacuchi.kafka.valley.domain.serializers.CustomJsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

@Component
public class MySerdesFactory {
    public static Serde<Transmissions> Transmissions() {
        CustomJsonSerializer<Transmissions> serializer = new CustomJsonSerializer<>();
        CustomJsonDeserializer<Transmissions> deserializer = new CustomJsonDeserializer<>(Transmissions.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<OperatorModel> OperatorModel() {
        JsonSerializer<OperatorModel> serializer = new JsonSerializer<>();
        JsonDeserializer<OperatorModel> deserializer = new JsonDeserializer<>(OperatorModel.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<VendorModel> VendorModel() {
        CustomJsonSerializer<VendorModel> serializer = new CustomJsonSerializer<>();
        CustomJsonDeserializer<VendorModel> deserializer = new CustomJsonDeserializer<>(VendorModel.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<SpeedLimiterModel> SpeedLimiterModel() {
        CustomJsonSerializer<SpeedLimiterModel> serializer = new CustomJsonSerializer<>();
        CustomJsonDeserializer<SpeedLimiterModel> deserializer = new CustomJsonDeserializer<>(SpeedLimiterModel.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }


    public static Serde<EnrichedTrackingSummary> EnrichedTrackingSummary() {
        CustomJsonSerializer<EnrichedTrackingSummary> serializer = new CustomJsonSerializer<>();
        CustomJsonDeserializer<EnrichedTrackingSummary> deserializer = new CustomJsonDeserializer<>(EnrichedTrackingSummary.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }


    public static Serde<EnrichedLimiterVendor> EnrichedLimiterVendor() {
        CustomJsonSerializer<EnrichedLimiterVendor> serializer = new CustomJsonSerializer<>();
        CustomJsonDeserializer<EnrichedLimiterVendor> deserializer = new CustomJsonDeserializer<>(EnrichedLimiterVendor.class);
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
