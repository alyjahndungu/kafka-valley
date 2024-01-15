package com.camacuchi.kafka.valley.config;


import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;

@Slf4j
@Configuration
@EnableKafkaStreams
public class TopicsConfig {
    @Bean
    public NewTopic transmissionTopic() {
        return TopicBuilder.name(EValleyTopics.TOPIC_TRANSMISSIONS.getName())
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic joinEventTopic() {
        return TopicBuilder.name(EValleyTopics.TOPIC_TRANSMISSIONS.getName())
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic enriched() {
        return TopicBuilder.name(EValleyTopics.TOPIC_ENRICHED_TRACKER_RESULT.getName())
                .partitions(3)
                .replicas(1)
                .build();
    }
}
