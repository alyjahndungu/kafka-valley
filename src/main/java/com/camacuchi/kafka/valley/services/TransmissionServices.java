package com.camacuchi.kafka.valley.services;

import com.camacuchi.kafka.valley.domain.enums.EStateStore;
import com.camacuchi.kafka.valley.domain.models.EnrichedLimiterVendor;
import com.camacuchi.kafka.valley.domain.models.EnrichedTrackingSummary;
import com.camacuchi.kafka.valley.domain.models.TransmissionCountDto;
import com.camacuchi.kafka.valley.domain.models.Transmissions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransmissionServices {

    private final StreamsBuilderFactoryBean streamsBuilder;

    public List<TransmissionCountDto> transmissionsCount() {
        ReadOnlyKeyValueStore<String, Long> transmissionStoreData = Objects.requireNonNull(streamsBuilder.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(
                        EStateStore.TRANSMISSION_COUNT_STORE.getName(),
                        QueryableStoreTypes.keyValueStore()
                ));

        var transmissions = transmissionStoreData.all();
        var spliterator = Spliterators.spliteratorUnknownSize(transmissions, 0);
        return StreamSupport.stream(spliterator, false)
                .map(data -> new TransmissionCountDto(data.key, data.value))
                .toList();
    }

    public List<Transmissions> getOverSpeeding() {
        ReadOnlyKeyValueStore<String, Transmissions> overSpeedingStoreData = Objects.requireNonNull(streamsBuilder.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(
                        EStateStore.OVER_SPEEDING_STORE.getName(),
                        QueryableStoreTypes.keyValueStore()
                ));

        KeyValueIterator<String, Transmissions> transmissions = overSpeedingStoreData.all();

        List<Transmissions> overSpeedingList = new ArrayList<>();
        while (transmissions.hasNext()) {
            KeyValue<String, Transmissions> next = transmissions.next();
            overSpeedingList.add(next.value);
        }

        log.info("Over Speeding Result: {}", overSpeedingList);
        return overSpeedingList;
    }

    public List<EnrichedTrackingSummary> getTrackingData() {
        ReadOnlyKeyValueStore<String, EnrichedTrackingSummary> enriched = Objects.requireNonNull(streamsBuilder.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(
                        "ENRICHED-TRACKED-DATA",
                        QueryableStoreTypes.keyValueStore()
                ));

        KeyValueIterator<String, EnrichedTrackingSummary> transmissions = enriched.all();

        List<EnrichedTrackingSummary> overSpeedingList = new ArrayList<>();
        while (transmissions.hasNext()) {
            KeyValue<String, EnrichedTrackingSummary> next = transmissions.next();
            overSpeedingList.add(next.value);
        }

        return overSpeedingList;
    }

    public Transmissions getTransmission(String name) {
        ReadOnlyKeyValueStore<String, Transmissions> storeData = Objects.requireNonNull(streamsBuilder.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(
                        EStateStore.OVER_SPEEDING_STORE.getName(),
                        QueryableStoreTypes.keyValueStore()
                ));
        Transmissions transmissions = storeData.get(name);

//        assert transmissions.latitude() != null;
//        assert transmissions.longitude() != null;
//        double km = calculateDistance(Double.valueOf(transmissions.latitude()), Double.valueOf(transmissions.longitude()), Double.valueOf(transmissions.latitude()), Double.valueOf(transmissions.longitude()));
//         log.info("Distance Travelled: {}", km);

        log.info("Vehicle Result: {}", transmissions);
        return transmissions;
    }

    static final int EARTH_RADIUS = 6371; // Radius of the earth in kilometers

    public Double calculateDistance(Double lat1, Double lon1, Double lat2, Double lon2){
        double latDistance = toRad(lat2-lat1);
        double lonDistance = toRad(lon2-lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
                Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
                        Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));

        return EARTH_RADIUS * c; // in kilometers
    }

    private static Double toRad(Double value) {
        return value * Math.PI / 180;
    }

}
