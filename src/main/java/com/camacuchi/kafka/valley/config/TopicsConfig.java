//package com.camacuchi.kafka.valley.config;
//
//
//import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.admin.NewTopic;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.annotation.EnableKafkaStreams;
//import org.springframework.kafka.config.TopicBuilder;
//
//@Slf4j
//@Configuration
//public class TopicsConfig {
//    @Bean
//    public NewTopic topicBuilder() {
//        return TopicBuilder.name(EValleyTopics.TOPIC_TRANSMISSIONS.getName())
//                .partitions(2)
//                .replicas(1)
//                .build();
//    }
//}
