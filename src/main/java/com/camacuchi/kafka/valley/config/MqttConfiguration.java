package com.camacuchi.kafka.valley.config;


import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

@Slf4j
@Configuration
public class MqttConfiguration {

    @Value("${MQTT_BROKER}")
    private String mqttBroker;

    @Value("${MQTT_SUB_USERNAME}")
    private String mqttUsername;

    @Value("${MQTT_SUB_PASSWORD}")
    private String mqttPassword;

    @Value("${MQTT_TOPIC}")
    private String mqttTopic;

    private final KafkaTemplate<String, Transmissions> kafkaTemplate;

    public MqttConfiguration(KafkaTemplate<String, Transmissions> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[]{mqttBroker});
        options.setUserName(mqttUsername);
        options.setPassword(mqttPassword.toCharArray());
        factory.setConnectionOptions(options);

        return factory;
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel mqttOutputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageProducer inbound() {
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter("mqttClient", mqttClientFactory(), mqttTopic);
        adapter.setOutputChannel(mqttInputChannel());
        return adapter;
    }

    @Bean
    public MessageHandler mqttOutbound() {
        MqttPahoMessageHandler messageHandler = new MqttPahoMessageHandler("mqttClient", mqttClientFactory());
        messageHandler.setAsync(true);
        messageHandler.setDefaultTopic(mqttTopic);
        return messageHandler;
    }


    @Bean
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public MessageHandler mqttInbound() {
        return message -> {
             Transmissions transmissions = transform(message.getPayload().toString());
             if ( transmissions != null && transmissions.imei() != null){
                 kafkaTemplate.send(EValleyTopics.TOPIC_TRANSMISSIONS.getName(), transmissions);
             }
        };
    }


    public Transmissions transform(String rawData) {
        return rawData.startsWith("{") ? deserializeData(rawData, new TypeReference<>() {}) : null;
    }

    private static  <T> T deserializeData(String json, TypeReference<T> typeReference)  {
           try {
               return new ObjectMapper().readValue(json, typeReference);
           }catch (JsonProcessingException jse){
               log.error("JsonProcessingException Error Message {}", jse.getMessage());
           }
        return null;
    }

}
