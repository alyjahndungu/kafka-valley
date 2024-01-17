package com.camacuchi.kafka.valley.topologies;

import com.camacuchi.kafka.valley.domain.enums.EStateStore;
import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
import com.camacuchi.kafka.valley.domain.models.*;
import com.camacuchi.kafka.valley.domain.serdes.MySerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Component
public class TransmissionTopology {

    private static final double SPEED_THRESHOLD = 85.0;

    final SpeedLimiterVendorJoiner speedLimiterVendorJoiner = new SpeedLimiterVendorJoiner();

    final GovernorTransmissionsJoiner governorTransmissionsJoiner = new GovernorTransmissionsJoiner();

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

       //stream speed-limiters topic and create a KTable
        final KTable<String, SpeedLimiterModel> speedLimiterTable = getSpeedLimiterModelKTable(streamsBuilder);

        //stream vendors topic and create a KTable
        final KTable<String, VendorModel> vendorModelTable = getVendorModelKTable(streamsBuilder);


        //creating a join between speed-limiters and vendors tables
        aggregateSpeedLimiterAndVendorTables(speedLimiterTable, vendorModelTable);

        //stream transmissions topic and creating a KTable
        final KTable<String, Transmissions> transmissionsKTable = getTransmissionsKTable(streamsBuilder);

        //streaming the topic of speed-limiter and vendor joined tables
        final KTable<String, EnrichedLimiterVendor> enrichedLimiterTable = getEnrichedLimiterVendorKTable(streamsBuilder);


        //joining the enriched vendor limiter records with Transmissions
        aggregationEnrichedLimiterVendorAndTransmissions(enrichedLimiterTable, transmissionsKTable);


//        filterOverSpeedingVehicles(streamsBuilder);



    }




    private static KTable<String, SpeedLimiterModel> getSpeedLimiterModelKTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.stream(EValleyTopics.TOPIC_SPEED_LIMITERS.getName(),
                                Consumed.with(Serdes.String(), MySerdesFactory.SpeedLimiterModel()))
                        .selectKey((key, value) -> value.speedLimiter().id())
                        .toTable(Materialized.<String, SpeedLimiterModel, KeyValueStore<Bytes, byte[]>>
                                        as("speed-limiter-model-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(MySerdesFactory.SpeedLimiterModel()));
    }
    
    private static KTable<String, VendorModel> getVendorModelKTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.stream(EValleyTopics.TOPIC_VENDORS.getName(),
                        Consumed.with(Serdes.String(), MySerdesFactory.VendorModel()))
                .map((key, value) -> new KeyValue<>(value.vendors().id(), value))
                .toTable(Materialized.<String, VendorModel, KeyValueStore<Bytes, byte[]>>
                                as("vendors-model-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(MySerdesFactory.VendorModel()));
    }


    private void aggregateSpeedLimiterAndVendorTables(KTable<String, SpeedLimiterModel> speedLimiterTable, KTable<String, VendorModel> vendorModelTable) {
        final KTable<String, EnrichedLimiterVendor> enrichedLimiterVendorKTable = speedLimiterTable.leftJoin(vendorModelTable,
                SpeedLimiterModel::getVendorId,
                speedLimiterVendorJoiner,
                Materialized.<String, EnrichedLimiterVendor, KeyValueStore<Bytes, byte[]>>
                                as("enriched-limiter-vendor-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(MySerdesFactory.EnrichedLimiterVendor())
        );

        //publishing the joined data into a new topic
        enrichedLimiterVendorKTable.toStream()
                .map((key, value) -> new KeyValue<>(value.limiterId(), value))
                .to(EValleyTopics.TOPIC_ENRICHED_TRACKER_RESULT.getName(), Produced.with(Serdes.String(), MySerdesFactory.EnrichedLimiterVendor()));
    }


    private static KTable<String, Transmissions> getTransmissionsKTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.stream(EValleyTopics.TOPIC_TRANSMISSIONS.getName(),
                                Consumed.with(Serdes.String(), MySerdesFactory.Transmissions()))
                        .selectKey((key, value) -> value.imei())
                        .toTable(Materialized.<String, Transmissions, KeyValueStore<Bytes, byte[]>>
                                        as("limiter-transmissions-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(MySerdesFactory.Transmissions()));
    }


    private static KTable<String, EnrichedLimiterVendor> getEnrichedLimiterVendorKTable(StreamsBuilder streamsBuilder) {
              return streamsBuilder.stream(EValleyTopics.TOPIC_ENRICHED_TRACKER_RESULT.getName(),
                                Consumed.with(Serdes.String(), MySerdesFactory.EnrichedLimiterVendor()))
                        .map((key, value) -> new KeyValue<>(value.limiterId(), value))
                        .toTable(Materialized.<String, EnrichedLimiterVendor, KeyValueStore<Bytes, byte[]>>
                                        as("table-speed-limiter-vendor-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(MySerdesFactory.EnrichedLimiterVendor()));

    }

    private void aggregationEnrichedLimiterVendorAndTransmissions(KTable<String, EnrichedLimiterVendor> enrichedLimiterTable, KTable<String, Transmissions> transmissionsKTable) {
        final KTable<String, EnrichedTrackingSummary> enrichedTrackingDataKTable = enrichedLimiterTable.leftJoin(transmissionsKTable,
                EnrichedLimiterVendor::limiterSerialNumber,
                governorTransmissionsJoiner,
                Materialized.<String, EnrichedTrackingSummary, KeyValueStore<Bytes, byte[]>>
                                as("ENRICHED-TRACKED-DATA")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(MySerdesFactory.EnrichedTrackingSummary()));

        //publish the joined table into a new kafka topic
        enrichedTrackingDataKTable.toStream()
                .selectKey((key, value) -> key)
                .peek((key,value) -> log.info("ENRICHED TRACKING DATA:  {}", value))
                .to(EValleyTopics.TOPIC_ENRICHED_TRANSMISSIONS.getName(), Produced.with(Serdes.String(), MySerdesFactory.EnrichedTrackingSummary()));
    }

    private static void filterOverSpeedingVehicles(StreamsBuilder streamsBuilder) {
        KStream<String, EnrichedTrackingSummary> highSpeedTransmissions =   streamsBuilder.stream(EValleyTopics.TOPIC_ENRICHED_TRACKER_RESULT.getName(),
                        Consumed.with(Serdes.String(), MySerdesFactory.EnrichedTrackingSummary()))
                .map((key, transmission) -> new KeyValue<>(transmission.imei(), transmission))
                .filter((key, transmission) ->  Double.parseDouble(Objects.requireNonNull(transmission.speed())) > SPEED_THRESHOLD)
                .groupByKey(Grouped.with(Serdes.String(), MySerdesFactory.EnrichedTrackingSummary()))
                .reduce((current, aggregate) -> aggregate,
                        Materialized.<String, EnrichedTrackingSummary, KeyValueStore<Bytes, byte[]>>as(EStateStore.OVER_SPEEDING_STORE.getName())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(MySerdesFactory.EnrichedTrackingSummary()))
                .toStream();

        highSpeedTransmissions.print(Printed.<String, EnrichedTrackingSummary>toSysOut().withLabel("OVER SPEEDING TRANSMISSIONS ->"));
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

}
