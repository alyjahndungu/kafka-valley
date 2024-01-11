package com.camacuchi.kafka.valley.topologies;

import com.camacuchi.kafka.valley.domain.enums.EStateStore;
import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
import com.camacuchi.kafka.valley.domain.models.*;
import com.camacuchi.kafka.valley.domain.serdes.JsonSerdes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;

@Component
public class TransmissionTopology {

    private static final double SPEED_THRESHOLD = 85.0;


    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
//        KStream<String, Transmissions> transmissionStream = streamsBuilder.stream(EValleyTopics.TOPIC_TRANSMISSIONS.getName(),
//                        Consumed.with(Serdes.String(), JsonSerdes.Transmissions()))
//                .selectKey((key, value) -> value.imei());

        KStream<String, SpeedLimiterModel> speedLimiterStream = streamsBuilder.stream(EValleyTopics.TOPIC_SPEED_LIMITERS.getName(),
                        Consumed.with(Serdes.String(), JsonSerdes.SpeedLimiterModel()))
                .selectKey((key, value) -> value.speedLimiter().id());

        KStream<String, VendorModel> vendors = streamsBuilder.stream(EValleyTopics.TOPIC_VENDORS.getName(),
                Consumed.with(Serdes.String(), JsonSerdes.VendorModel())).selectKey((key, value) -> value.vendors().id());

        KStream<String, OperatorModel> operators = streamsBuilder.stream(EValleyTopics.TOPIC_OPERATORS.getName(),
                        Consumed.with(Serdes.String(), JsonSerdes.OperatorModel())).selectKey((key, value) -> value.operators().id());

//
//        operators.print(Printed.<String, OperatorModel>toSysOut().withLabel("Streaming  Operators -> "));
//
//
//        vendors.print(Printed.<String, VendorModel>toSysOut().withLabel("Streaming  Vendors -> "));

//        transmissionStream.print(Printed.<String, Transmissions>toSysOut().withLabel("Streaming  Transmissions -> "));

//        transmissionsCount(transmissionStream);
//        onlineDevicesCount(transmissionStream);
//        highSpeedTransmissions(transmissionStream);

        convertIntoGlobalKTable(speedLimiterStream, vendors);
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
//        transmissionCount.toStream()
//                .print(Printed.<String, Long>toSysOut().withLabel("Transmission Count"));
//    }

    private static void onlineDevicesCount(KStream<String, Transmissions> transmissionStream) {
        KTable<String, Long> connectedCount = transmissionStream
                .map((key, transmissions) -> KeyValue.pair(transmissions.imei(), transmissions))
                .filter((key, transmission) ->   Objects.requireNonNull(transmission.signal()).contains("connected"))
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.Transmissions()))
                .count(Named.as(EStateStore.CONNECTED_TRANSMISSION_COUNT_STORE.getName()),
                        Materialized.as(EStateStore.CONNECTED_TRANSMISSION_COUNT_STORE.getName()));
        connectedCount.toStream();
        transmissionStream.print(Printed.<String, Transmissions>toSysOut().withLabel("Online Devices Count ->"));

//        speedLimitersTable.toStream();
//        speedLimiterModelKStream.print(Printed.<String, SpeedLimiterModel>toSysOut().withLabel("Speed Limiter Stream ->"));
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

    private static void convertIntoGlobalKTable(KStream<String, SpeedLimiterModel> speedLimiterStream, KStream<String, VendorModel> vendorModelKStream) {

        KTable<String, SpeedLimiterModel> speedLimitersTable = speedLimiterStream
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.SpeedLimiterModel()))
                .reduce((value1, value2) -> value2, Materialized.as("speed-limiters-store"));

        KTable<String, VendorModel> vendorModelTable = vendorModelKStream
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.VendorModel()))
                .reduce((value1, value2) -> value2, Materialized.as("vendors-store"));

        KTable<String, JoinedDataTable> joinedTable = speedLimitersTable
                .leftJoin(vendorModelTable, JoinedDataTable::new);

        joinedTable.toStream().print(Printed.<String, JoinedDataTable>toSysOut().withLabel("JoinedTable"));

        joinedTable.toStream().to(EValleyTopics.TOPIC_JOIN_EVENTS.getName(), Produced.with(Serdes.String(), JsonSerdes.JoinedDataTable()));

    }

}
