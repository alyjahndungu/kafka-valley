//package com.camacuchi.kafka.valley.topologies;
//
//import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
//import com.camacuchi.kafka.valley.domain.models.Transmissions;
//import com.camacuchi.kafka.valley.serdes.JsonSerdes;
//import com.camacuchi.kafka.valley.serdes.TransmissionsSerde;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.common.utils.Bytes;
//import org.apache.kafka.streams.KeyValue;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.Topology;
//import org.apache.kafka.streams.kstream.*;
//import org.apache.kafka.streams.state.KeyValueStore;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Bean;
//import org.springframework.kafka.support.serializer.JsonSerde;
//import org.springframework.stereotype.Component;
//
//import java.util.Objects;
//
//@Component
//@Slf4j
//public class TransmissionTopology {
//
//    private static final double SPEED_THRESHOLD = 85.0;
//    private static final  String OVER_SPEEDING_STORE = "over_speeding";
//
////    @Autowired
////    public static Topology build() {
////        StreamsBuilder builder = new StreamsBuilder();
////        KStream<String, Transmissions> transmissionStreams = builder.stream(EValleyTopics.TOPIC_TRANSMISSIONS.getName(),
////                        Consumed.with(Serdes.String(), JsonSerdes.Transmissions()))
////                .selectKey((key, value) -> value.imei());
////
////        transmissionStreams.print(Printed.<String, Transmissions>toSysOut().withLabel("TRANSMISSIONS STREAM"));
////        processOverSpeeding(transmissionStreams);
////        return builder.build();
////    }
//
//    @Autowired
//    public void process(StreamsBuilder streamsBuilder) {
//        KStream<String, Transmissions> transmissionStreams = streamsBuilder.stream(EValleyTopics.TOPIC_TRANSMISSIONS.getName(),
//                        Consumed.with(Serdes.String(), JsonSerdes.Transmissions()))
//                .selectKey((key, value) -> value.imei());
//
//        transmissionStreams.print(Printed.<String, Transmissions>toSysOut().withLabel("TRANSMISSIONS STREAM"));
//        processOverSpeeding(transmissionStreams);
//    }
//
//    private static void processOverSpeeding(KStream<String, Transmissions> generalOrdersStream) {
//        KStream<String, Transmissions> highSpeedTransmissions = generalOrdersStream
//                .map((key, transmission) -> KeyValue.pair(transmission.imei(), transmission))
//                .filter((key, transmission) ->  Objects.requireNonNull(transmission.speed()) > SPEED_THRESHOLD)
//                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Transmissions.class)))
//                .reduce((current, aggregate) -> aggregate,
//                        Materialized.<String, Transmissions, KeyValueStore<Bytes, byte[]>>as(OVER_SPEEDING_STORE)
//                                .withKeySerde(Serdes.String())
//                                .withValueSerde(new TransmissionsSerde()))
//                .toStream();
//
//        highSpeedTransmissions.print(Printed.<String, Transmissions>toSysOut().withLabel("OVER SPEEDING TRANSMISSIONS ->"));
//    }
//
//
//
//}
