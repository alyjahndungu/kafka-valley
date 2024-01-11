package com.camacuchi.kafka.valley.controllers;

import com.camacuchi.kafka.valley.domain.models.TransmissionCountDto;
import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.camacuchi.kafka.valley.services.TransmissionServices;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v1/transmissions")
public class TransmissionControllers {

    private final TransmissionServices transmissionService;

    @GetMapping("/count")
    public List<TransmissionCountDto> transmissionCount() {
        return transmissionService.transmissionsCount();
    }

    @GetMapping("/speeding")
    public List<Transmissions> overSpeeding() {
      return   transmissionService.getOverSpeeding();
    }

    @GetMapping("/imei")
    public Transmissions getTransmission() {
        return transmissionService.getTransmission("KCC647M");
    }

}
