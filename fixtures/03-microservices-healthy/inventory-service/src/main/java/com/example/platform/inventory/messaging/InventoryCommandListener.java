package com.example.platform.inventory.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class InventoryCommandListener {

    private final StockService stockService;
    private final Inbox inbox;
    private final ObjectMapper json;

    public InventoryCommandListener(StockService stockService, Inbox inbox, ObjectMapper json) {
        this.stockService = stockService;
        this.inbox = inbox;
        this.json = json;
    }

    @KafkaListener(topics = "inventory-commands")
    @Transactional
    public void onCommand(ConsumerRecord<String, String> record) throws JsonProcessingException {
        if (!inbox.firstDelivery(Headers.messageId(record))) {
            return;
        }
        switch (Headers.messageType(record)) {
            case "ReserveStock" -> stockService.reserve(json.readValue(record.value(), Messages.ReserveStock.class));
            case "ReleaseStock" -> stockService.release(json.readValue(record.value(), Messages.ReleaseStock.class));
            default -> throw new IllegalArgumentException("Unknown command " + Headers.messageType(record));
        }
    }
}
