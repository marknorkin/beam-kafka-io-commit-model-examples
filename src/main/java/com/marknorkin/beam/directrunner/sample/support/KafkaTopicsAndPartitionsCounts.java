package com.marknorkin.beam.directrunner.sample.support;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
@Data
public class KafkaTopicsAndPartitionsCounts {
    private final List<KafkaTopicAndPartitionCount> kafkaTopicsAndPartitionCounts;
    private final boolean partitionsCountSpecifiedInAllTopics;
    private final boolean partitionsCountSpecifiedInAnyOfTheTopics;

    public KafkaTopicsAndPartitionsCounts(List<KafkaTopicAndPartitionCount> kafkaTopicsAndPartitionCounts) {
        this.kafkaTopicsAndPartitionCounts = kafkaTopicsAndPartitionCounts;
        this.partitionsCountSpecifiedInAllTopics =
            kafkaTopicsAndPartitionCounts.stream().allMatch(KafkaTopicAndPartitionCount::isPartitionsCountSpecified);
        this.partitionsCountSpecifiedInAnyOfTheTopics =
            kafkaTopicsAndPartitionCounts.stream().anyMatch(KafkaTopicAndPartitionCount::isPartitionsCountSpecified);
    }

    public KafkaTopicsAndPartitionsCounts(String topic, Integer partitionsCount) {
        this(Collections.singletonList(new KafkaTopicAndPartitionCount(topic, partitionsCount)));
    }

    public List<String> getTopicsNames() {
        return kafkaTopicsAndPartitionCounts.stream()
            .map(KafkaTopicAndPartitionCount::getTopic)
            .collect(Collectors.toList());
    }

    public List<TopicPartition> toTopicPartitions() {
        if (!partitionsCountSpecifiedInAllTopics) {
            throw new IllegalStateException(
                "Can not convert to topic partitions as not all topic specified partitions count");
        }
        return kafkaTopicsAndPartitionCounts.stream().flatMap(this::convertTopic).collect(Collectors.toList());
    }

    private Stream<TopicPartition> convertTopic(KafkaTopicAndPartitionCount kafkaTopicAndPartitionCount) {
        return IntStream.range(0, kafkaTopicAndPartitionCount.getPartitionsCount().get())
            .mapToObj(it -> new TopicPartition(kafkaTopicAndPartitionCount.getTopic(), it));
    }
}
