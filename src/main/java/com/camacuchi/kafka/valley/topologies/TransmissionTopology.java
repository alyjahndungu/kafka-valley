package com.camacuchi.kafka.valley.topologies;

import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.camacuchi.kafka.valley.serdes.JsonSerdes;
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
    private static final  String TRANSMISSION_COUNT_STORE = "transmissions_count";
    private static final  String OVER_SPEEDING_STORE = "over_speeding";


    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        KStream<String, Transmissions> transmissionStream = streamsBuilder.stream(EValleyTopics.TOPIC_TRANSMISSIONS.getName(),
                        Consumed.with(Serdes.String(), JsonSerdes.Transmissions()))
                .selectKey((key, value) -> value.imei());
        transmissionStream.print(Printed.<String, Transmissions>toSysOut().withLabel("Streaming -> "));

        transmissionsCount(transmissionStream);
        highSpeedTransmissions(transmissionStream);
    }

    private static void transmissionsCount(KStream<String, Transmissions> transmissionStream) {

        KTable<String, Long> transmissionCount = transmissionStream.map(
                        (key, transmissions) -> KeyValue.pair(transmissions.imei(), transmissions))
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.Transmissions()))
                .count(Named.as(TRANSMISSION_COUNT_STORE),
                        Materialized.as(TRANSMISSION_COUNT_STORE));

        transmissionCount.toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(TRANSMISSION_COUNT_STORE));
    }

    private  static  void highSpeedTransmissions(KStream<String, Transmissions> transmissionStream) {
        KStream<String, Transmissions> highSpeedTransmissions = transmissionStream
                .map((key, transmission) -> KeyValue.pair(transmission.imei(), transmission))
                .filter((key, transmission) ->  Objects.requireNonNull(transmission.speed()) > SPEED_THRESHOLD)
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.Transmissions()))
                .reduce((current, aggregate) -> aggregate,
                        Materialized.<String, Transmissions, KeyValueStore<Bytes, byte[]>>as(OVER_SPEEDING_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.Transmissions()))
                .toStream();

        highSpeedTransmissions.print(Printed.<String, Transmissions>toSysOut().withLabel("OVER SPEEDING TRANSMISSIONS ->"));
    }


}
