//package com.camacuchi.kafka.valley.services;
//
//import com.camacuchi.kafka.valley.domain.models.TransmissionCountDto;
//import com.camacuchi.kafka.valley.domain.models.Transmissions;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.streams.KeyValue;
//import org.apache.kafka.streams.StoreQueryParameters;
//import org.apache.kafka.streams.state.KeyValueIterator;
//import org.apache.kafka.streams.state.QueryableStoreTypes;
//import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.kafka.config.StreamsBuilderFactoryBean;
//import org.springframework.stereotype.Service;
//
//import java.util.*;
//import java.util.stream.StreamSupport;
//
//@Slf4j
//@Service
//public class TransmissionServices {
//    @Autowired
//    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;
//    private static final Logger LOGGER = LoggerFactory.getLogger(TransmissionServices.class);
//    private static final  String TRANSMISSION_COUNT_STORE = "transmissions_count";
//    private static final  String OVER_SPEEDING_STORE = "over_speeding";
//    public List<TransmissionCountDto> transmissionsCount() {
//        ReadOnlyKeyValueStore<String, Long> transmissionStoreData = Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams())
//                .store(StoreQueryParameters.fromNameAndType(
//                        TRANSMISSION_COUNT_STORE,
//                        QueryableStoreTypes.keyValueStore()
//                ));
//
//        var transmissions = transmissionStoreData.all();
//        var spliterator = Spliterators.spliteratorUnknownSize(transmissions, 0);
//        return StreamSupport.stream(spliterator, false)
//                .map(data -> new TransmissionCountDto(data.key, data.value))
//                .toList();
//    }
//
//    public Map<String, Transmissions> getOverSpeeding() {
//        Map<String, Transmissions> overSpeedingMap = new HashMap<>();
//
//        ReadOnlyKeyValueStore<String, Transmissions> overSpeedingStoreData = Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams())
//                .store(StoreQueryParameters.fromNameAndType(
//                        OVER_SPEEDING_STORE,
//                        QueryableStoreTypes.keyValueStore()
//                ));
//
//        try (KeyValueIterator<String, Transmissions> transmissions = overSpeedingStoreData.all()) {
//
//            while (transmissions.hasNext()) {
//                KeyValue<String, Transmissions> kv = transmissions.next();
//                overSpeedingMap.put(kv.key, kv.value);
//            }
//
//            log.info("Over Speeding Result: {}", overSpeedingMap);
//            return overSpeedingMap;
//        }
//    }
//
//    public Transmissions getTransmission(String imei) {
//     Transmissions transmissions = (Transmissions) Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams()).store(StoreQueryParameters.fromNameAndType(
//                        OVER_SPEEDING_STORE,
//                        QueryableStoreTypes.keyValueStore()
//                )).get(imei);
//        log.info(" Imei Result: {}", transmissions);
//        return transmissions;
//    }
//
//}
