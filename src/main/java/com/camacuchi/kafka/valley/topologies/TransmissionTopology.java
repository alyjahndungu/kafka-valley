package com.camacuchi.kafka.valley.topologies;

import com.camacuchi.kafka.valley.domain.enums.EStateStore;
import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
import com.camacuchi.kafka.valley.domain.models.*;
import com.camacuchi.kafka.valley.domain.serdes.MySerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Slf4j
@Component
public class TransmissionTopology {

    private static final double SPEED_THRESHOLD = 85.0;

    final LimiterVendorJoiner limiterVendorJoiner = new LimiterVendorJoiner();
    final SpeedLimiterVendorJoiner speedLimiterVendorJoiner = new SpeedLimiterVendorJoiner();

    final GovernerTransmissionsJoiner governerTransmissionsJoiner = new GovernerTransmissionsJoiner();



    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
//        KStream<String, Transmissions> transmissionStream = streamsBuilder.stream(EValleyTopics.TOPIC_TRANSMISSIONS.getName(),
//                        Consumed.with(Serdes.String(), JsonSerdes.Transmissions()))
//                .selectKey((key, value) -> value.imei());

        KStream<String, SpeedLimiterModel> speedLimiterStream = streamsBuilder.stream(EValleyTopics.TOPIC_SPEED_LIMITERS.getName(),
                        Consumed.with(Serdes.String(), MySerdesFactory.SpeedLimiterModel()))
                .map((key, value) -> new KeyValue<>(value.speedLimiter(), value))
                .selectKey((key, value) -> value.speedLimiter().id());
//
//        KStream<String, VendorModel> vendors = streamsBuilder.stream(EValleyTopics.TOPIC_VENDORS.getName(),
//                Consumed.with(Serdes.String(), MySerdesFactory.VendorModel()))
//                .map((key, value) -> new KeyValue<>(value.vendors(), value))
//                .selectKey((key, value) -> value.vendors().id());

//        KStream<String, OperatorModel> operators = streamsBuilder.stream(EValleyTopics.TOPIC_OPERATORS.getName(),
//                        Consumed.with(Serdes.String(), JsonSerdes.OperatorModel())).selectKey((key, value) -> value.operators().id());



        final KTable<String, SpeedLimiterModel> speedLimiterTable =
                streamsBuilder.stream(EValleyTopics.TOPIC_SPEED_LIMITERS.getName(),
                                Consumed.with(Serdes.String(), MySerdesFactory.SpeedLimiterModel()))
                        .selectKey((key, value) -> value.speedLimiter().id())
                        .toTable(Materialized.<String, SpeedLimiterModel, KeyValueStore<Bytes, byte[]>>
                                        as("speed-limiter-model-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(MySerdesFactory.SpeedLimiterModel()));



        final KTable<String, VendorModel> vendorModelTable =
                streamsBuilder.stream(EValleyTopics.TOPIC_VENDORS.getName(),
                                Consumed.with(Serdes.String(), MySerdesFactory.VendorModel()))
                        .map((key, value) -> new KeyValue<>(value.vendors().id(), value))
                        .toTable(Materialized.<String, VendorModel, KeyValueStore<Bytes, byte[]>>
                                        as("vendors-model-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(MySerdesFactory.VendorModel()));


//        final KTable<String, JoinedDataTable> joinedDataTableKTable = speedLimiterTable.join(vendorModelTable,
//                SpeedLimiterModel::getVendorId,
//                limiterVendorJoiner);


        final KTable<String, EnrichedLimiterVendor> enrichedLimiterVendorKTable = speedLimiterTable.leftJoin(vendorModelTable,

         SpeedLimiterModel::getVendorId,
         speedLimiterVendorJoiner,
                Materialized.<String, EnrichedLimiterVendor, KeyValueStore<Bytes, byte[]>>
                                as("EMP-DEPT-MV")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(MySerdesFactory.EnrichedLimiterVendor())
        );


        enrichedLimiterVendorKTable.toStream()
                .map((key, value) -> new KeyValue<>(value.limiterId(), value))
                .peek((key,value) -> System.out.println("(enrichedLimiterVendorKTable) key,value = " + key  + "," + value.toString()))
                .to(EValleyTopics.TOPIC_ENRICHED_TRACKER_RESULT.getName(), Produced.with(Serdes.String(), MySerdesFactory.EnrichedLimiterVendor()));

        final KTable<String, Transmissions> transmissionsKTable =
                streamsBuilder.stream(EValleyTopics.TOPIC_TRANSMISSIONS.getName(),
                                Consumed.with(Serdes.String(), MySerdesFactory.Transmissions()))
                        .selectKey((key, value) -> value.imei())
                        .toTable(Materialized.<String, Transmissions, KeyValueStore<Bytes, byte[]>>
                                        as("limiter-transmissions-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(MySerdesFactory.Transmissions()));


        final KTable<String, EnrichedLimiterVendor> enrichedLimiterTable =
                streamsBuilder.stream(EValleyTopics.TOPIC_ENRICHED_TRACKER_RESULT.getName(),
                                Consumed.with(Serdes.String(), MySerdesFactory.EnrichedLimiterVendor()))
                        .map((key, value) -> new KeyValue<>(value.limiterId(), value))
                        .toTable(Materialized.<String, EnrichedLimiterVendor, KeyValueStore<Bytes, byte[]>>
                                        as("enriched-limiter-vendor-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(MySerdesFactory.EnrichedLimiterVendor()));


        final KTable<String, EnrichedTrackingData> enrichedTrackingDataKTable = enrichedLimiterTable.leftJoin(transmissionsKTable,
                 EnrichedLimiterVendor::limiterSerialNumber,
                governerTransmissionsJoiner,
                Materialized.<String, EnrichedTrackingData, KeyValueStore<Bytes, byte[]>>
                                as("ENRICHED-TRACKED-DATA")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(MySerdesFactory.EnrichedTrackingData())
        );


        enrichedTrackingDataKTable.toStream()
                .map((key, value) -> new KeyValue<>(value.getLimiterVendor().limiterId(), value))
                .peek((key,value) -> log.info("ENRICHED TRACKING DATA: Limiters {} and Transmissions {}", value.getLimiterVendor(), value.getTransmission()))
                .to(EValleyTopics.TOPIC_ENRICHED_TRANSMISSIONS.getName(), Produced.with(Serdes.String(), MySerdesFactory.EnrichedTrackingData()));


//
//        operators.print(Printed.<String, OperatorModel>toSysOut().withLabel("Streaming  Operators -> "));
//
//
//        vendors.print(Printed.<String, VendorModel>toSysOut().withLabel("Streaming  Vendors -> "));

//        transmissionStream.print(Printed.<String, Transmissions>toSysOut().withLabel("Streaming  Transmissions -> "));

//        transmissionsCount(transmissionStream);

//        onlineDevicesCount(transmissionStream);
//        highSpeedTransmissions(transmissionStream);

//        convertIntoGlobalKTable(speedLimiterStream, vendors);
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
                .groupByKey(Grouped.with(Serdes.String(), MySerdesFactory.Transmissions()))
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
                .groupByKey(Grouped.with(Serdes.String(), MySerdesFactory.Transmissions()))
                .reduce((current, aggregate) -> aggregate,
                        Materialized.<String, Transmissions, KeyValueStore<Bytes, byte[]>>as(EStateStore.OVER_SPEEDING_STORE.getName())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(MySerdesFactory.Transmissions()))
                .toStream();

        highSpeedTransmissions.print(Printed.<String, Transmissions>toSysOut().withLabel("OVER SPEEDING TRANSMISSIONS ->"));
    }

    private static void convertIntoGlobalKTable(KStream<String, SpeedLimiterModel> speedLimiterStream, KStream<String, VendorModel> vendorModelKStream) {

        KTable<String, SpeedLimiterModel> speedLimitersTable = speedLimiterStream
                .groupByKey(Grouped.with(Serdes.String(), MySerdesFactory.SpeedLimiterModel()))
                .reduce((value1, value2) -> value2, Materialized.as("speed-limiters-store"));



        KTable<String, VendorModel> vendorModelTable = vendorModelKStream
                .groupByKey(Grouped.with(Serdes.String(), MySerdesFactory.VendorModel()))
                .reduce((value1, value2) -> value2, Materialized.as("vendors-store"));

//        KTable<String, JoinedDataTable> joinedTable = speedLimitersTable
//                .leftJoin(vendorModelTable, JoinedDataTable::new);
//
//        joinedTable.toStream().print(Printed.<String, JoinedDataTable>toSysOut().withLabel("JoinedTable"));

//        joinedTable.toStream().to(EValleyTopics.TOPIC_JOIN_EVENTS.getName(), Produced.with(Serdes.String(), MySerdesFactory.JoinedDataTable()));

    }

}
