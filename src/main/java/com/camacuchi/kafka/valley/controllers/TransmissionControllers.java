package com.camacuchi.kafka.valley.controllers;

import com.camacuchi.kafka.valley.domain.enums.EValleyTopics;
import com.camacuchi.kafka.valley.domain.models.TransmissionCountDto;
import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.camacuchi.kafka.valley.services.TransmissionServices;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/transmissions")
public class TransmissionControllers {

    @Autowired
    private TransmissionServices transmissionService;

    private final KafkaTemplate<String, Transmissions> kafkaTemplate;

    public TransmissionControllers(KafkaTemplate<String, Transmissions> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/count")
    public List<TransmissionCountDto> transmissionCount() {
        return transmissionService.transmissionsCount();
    }

    @PostMapping()
    public void post(@RequestBody Transmissions transmissions) {
        kafkaTemplate.send(EValleyTopics.TOPIC_TRANSMISSIONS.getName(), transmissions);
    }

    @GetMapping("/speeding")
    public List<Transmissions> overSpeeding() {
      return   transmissionService.getOverSpeeding();
    }

    @GetMapping("/imei")
    public Transmissions getTransmission() {
        return transmissionService.getTransmission("34W39012345678");
    }

}
