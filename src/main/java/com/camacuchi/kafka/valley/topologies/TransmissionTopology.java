package com.camacuchi.kafka.valley.topologies;

import com.camacuchi.kafka.valley.domain.enums.EStateStore;
import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.camacuchi.kafka.valley.domain.serdes.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class TransmissionTopology {

    private static final double SPEED_THRESHOLD = 85.0;

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        KStream<String, Transmissions> transmissionStream = streamsBuilder.stream(EValleyTopics.TOPIC_TRANSMISSIONS.getName(),
                        Consumed.with(Serdes.String(), JsonSerdes.Transmissions()))
                .selectKey((key, value) -> value.imei());

        KStream<String, String > limiters = streamsBuilder.stream(EValleyTopics.TOPIC_LIMITERS.getName(),
                        Consumed.with(Serdes.String(), Serdes.String()))
                .selectKey((key, value) -> value);

        limiters.print(Printed.<String, String>toSysOut().withLabel("Streaming  Limiters -> "));


//        transmissionStream.print(Printed.<String, Transmissions>toSysOut().withLabel("Streaming  Transmissions -> "));

//        transmissionsCount(transmissionStream);
        onlineDevicesCount(transmissionStream);
        highSpeedTransmissions(transmissionStream);
    }

//    private static void transmissionsCount(KStream<String, Transmissions> transmissionStream) {
//
//        KTable<String, Long> transmissionCount = transmissionStream.map(
//                        (key, transmissions) -> KeyValue.pair(transmissions.imei(), transmissions))
//                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.Transmissions()))
//                .count(Named.as(EStateStore.TRANSMISSION_COUNT_STORE.getName()),
//                        Materialized.as(EStateStore.TRANSMISSION_COUNT_STORE.getName()));
//
////
////        transmissionCount.toStream()
////                .print(Printed.<String, Long>toSysOut().withLabel("Transmission Count"));
//    }

    private static void onlineDevicesCount(KStream<String, Transmissions> transmissionStream) {

        KTable<String, Long> connectedCount = transmissionStream
                .map((key, transmissions) -> KeyValue.pair(transmissions.imei(), transmissions))
                .filter((key, transmission) ->   Objects.requireNonNull(transmission.signal()).contains("connected"))
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.Transmissions()))
                .count(Named.as(EStateStore.CONNECTED_TRANSMISSION_COUNT_STORE.getName()),
                        Materialized.as(EStateStore.CONNECTED_TRANSMISSION_COUNT_STORE.getName()));
        connectedCount.toStream()
                .print(Printed.<String, Long>toSysOut().withLabel("Online Devices Count"));
    }


    private  static  void highSpeedTransmissions(KStream<String, Transmissions> transmissionStream) {
        KStream<String, Transmissions> highSpeedTransmissions = transmissionStream
                .map((key, transmission) -> KeyValue.pair(transmission.imei(), transmission))
                .filter((key, transmission) ->  Double.parseDouble(Objects.requireNonNull(transmission.speed())) > SPEED_THRESHOLD)
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.Transmissions()))
                .reduce((current, aggregate) -> aggregate,
                        Materialized.<String, Transmissions, KeyValueStore<Bytes, byte[]>>as(EStateStore.OVER_SPEEDING_STORE.getName())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.Transmissions()))
                .toStream();

        highSpeedTransmissions.print(Printed.<String, Transmissions>toSysOut().withLabel("OVER SPEEDING TRANSMISSIONS ->"));
    }




}
