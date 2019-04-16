package com.marknorkin.beam.kafkaio.commit.model.examples.support;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class VoidSerializer implements Serializer<Void> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Void data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
