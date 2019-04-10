package com.marknorkin.beam.directrunner.sample.support;

import lombok.Data;

import java.util.Optional;

@Data
public class KafkaTopicAndPartitionCount {
    private final String topic;
    private final Optional<Integer> partitionsCount;

    public KafkaTopicAndPartitionCount(String topic, Integer partitionsCount) {
        this.topic = topic;
        this.partitionsCount = Optional.ofNullable(partitionsCount);
    }

    public boolean isPartitionsCountSpecified() {
        return partitionsCount.isPresent();
    }
}
