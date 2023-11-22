package com.camacuchi.kafka.valley.config;

import com.camacuchi.kafka.valley.domain.enums.EStateStore;
import com.camacuchi.kafka.valley.domain.models.Transmissions;
import com.camacuchi.kafka.valley.services.TransmissionServices;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class WebSocketHandlerConfig extends TextWebSocketHandler {
    public WebSocketHandlerConfig() {
    }

    private  TransmissionServices transmissionService;

    public WebSocketHandlerConfig(TransmissionServices transmissionService) {
        this.transmissionService = transmissionService;
    }

    @Override
    public void handleMessage(WebSocketSession session, WebSocketMessage<?> message) throws IOException {
// Handle incoming messages here
        List<Transmissions> overSpeeding = transmissionService.getOverSpeeding();

// Process the message and send a response if needed
        session.sendMessage(new TextMessage("Received: " + overSpeeding));
    }
    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
// Perform actions when a new WebSocket connection is established
    }
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
// Perform actions when a WebSocket connection is closed
    }


}