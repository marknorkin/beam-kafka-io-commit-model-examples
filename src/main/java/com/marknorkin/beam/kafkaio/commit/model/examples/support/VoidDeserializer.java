package com.marknorkin.beam.kafkaio.commit.model.examples.support;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * KafkaIO needs withKeyDeserializer, so VoidDeserializer was added like a stub
 */
public class VoidDeserializer implements Deserializer<Void> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public Void deserialize(String s, byte[] bytes) {
        return null;
    }

    @Override
    public void close() {
    }
}
