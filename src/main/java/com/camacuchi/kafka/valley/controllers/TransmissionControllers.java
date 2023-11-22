package com.camacuchi.kafka.valley.controllers;

import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
import com.camacuchi.kafka.valley.domain.models.TransmissionCountDto;
import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.camacuchi.kafka.valley.services.TransmissionServices;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v1/transmissions")
public class TransmissionControllers {

    private final TransmissionServices transmissionService;

    private final KafkaTemplate<String, Transmissions> kafkaTemplate;

    @GetMapping("/count")
    public List<TransmissionCountDto> transmissionCount() {
        return transmissionService.transmissionsCount();
    }

    @PostMapping()
    public void post(@RequestBody Transmissions transmissions) {
        kafkaTemplate.send(EValleyTopics.TOPIC_TRANSMISSIONS.getName(), transmissions);
    }

//    @GetMapping("/speeding")
//    public List<Transmissions> overSpeeding() {
//      return   transmissionService.getOverSpeeding();
//    }

    @GetMapping("/imei")
    public Transmissions getTransmission() {
        return transmissionService.getTransmission("34W39012345678");
    }

}
