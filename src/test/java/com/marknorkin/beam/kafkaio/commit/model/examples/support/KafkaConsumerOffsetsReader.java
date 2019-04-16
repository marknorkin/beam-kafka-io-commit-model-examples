package com.marknorkin.beam.kafkaio.commit.model.examples.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Slf4j
public class KafkaConsumerOffsetsReader implements AutoCloseable {
    private final AdminClient adminClient;

    public KafkaConsumerOffsetsReader(String kafkaContainerBootstrapServers) {
        this.adminClient = KafkaAdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainerBootstrapServers));
    }

    public Map<TopicPartition, Long> getConsumerGroupTopicPartitionOffsets(String consumerGroup) {
        return getConsumerGroupTopicPartitionOffsetsAndMetadata(consumerGroup)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    private Map<TopicPartition, OffsetAndMetadata> getConsumerGroupTopicPartitionOffsetsAndMetadata(String consumerGroup) {
        try {
            ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets(consumerGroup);
            KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> topicPartitionOffsetAndMetadataFuture = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata();
            Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadata = topicPartitionOffsetAndMetadataFuture.get(1, TimeUnit.SECONDS);
            log.debug(topicPartitionOffsetAndMetadata.toString());
            return topicPartitionOffsetAndMetadata;
        } catch (InterruptedException e) {
            log.warn(e.getMessage(), e);
            Thread.currentThread().interrupt();
            return Collections.emptyMap();
        } catch (ExecutionException | TimeoutException e) {
            log.warn(e.getMessage(), e);
            return Collections.emptyMap();
        }
    }

    @Override
    public void close() {
        adminClient.close();
    }
}
