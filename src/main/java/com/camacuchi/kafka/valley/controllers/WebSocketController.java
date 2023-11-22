package com.camacuchi.kafka.valley.controllers;

import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.camacuchi.kafka.valley.services.TransmissionServices;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class WebSocketController {
    private final TransmissionServices transmissionService;


    @MessageMapping("/speeding")
    @SendTo("/topic/speeding")
    public ResponseEntity<List<Transmissions>> findSpeedings() {
        List<Transmissions> overSpeeding = transmissionService.getOverSpeeding();
        return ResponseEntity
                .ok(overSpeeding);
    }
}
