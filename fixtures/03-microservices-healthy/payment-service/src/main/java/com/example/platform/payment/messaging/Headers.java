package com.example.platform.payment.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

public final class Headers {

    private Headers() {
    }

    public static String messageId(ConsumerRecord<?, ?> record) {
        return value(record, "message-id");
    }

    public static String messageType(ConsumerRecord<?, ?> record) {
        return value(record, "message-type");
    }

    private static String value(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing header " + name + " on " + record.topic() + "@" + record.offset());
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
