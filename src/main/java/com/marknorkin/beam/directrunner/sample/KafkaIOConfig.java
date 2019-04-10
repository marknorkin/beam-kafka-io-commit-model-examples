package com.marknorkin.beam.directrunner.sample;

import com.google.common.collect.ImmutableMap;
import com.marknorkin.beam.directrunner.sample.options.KafkaConsumerOptions;
import com.marknorkin.beam.directrunner.sample.options.KafkaProducerOptions;
import com.marknorkin.beam.directrunner.sample.support.KafkaTopicsAndPartitionsCounts;
import com.marknorkin.beam.directrunner.sample.support.KafkaValueReader;
import com.marknorkin.beam.directrunner.sample.support.VoidDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

@Slf4j
public final class KafkaIOConfig {

    private KafkaIOConfig() {
    }

    public static PTransform<PCollection<String>, PDone> write(
            KafkaProducerOptions options, String outputTopic, String sinkGroupId, Integer partitionNumber) {

        int numShards = partitionNumber != null ? partitionNumber : getNumShardsForTopic(outputTopic, options);

        return KafkaIO.<Void, String>write()
            .withBootstrapServers(options.getKafkaBootstrapServers())
            .withTopic(outputTopic)
            .withEOS(numShards, sinkGroupId + UUID.randomUUID().toString())
            .updateProducerProperties(new ImmutableMap.Builder<String, Object>()
                .put(ACKS_CONFIG, "all")
                .build())
            .withProducerFactoryFn(new ProducerFactoryFn())
            .withValueSerializer(StringSerializer.class).values();
    }

    private static int getNumShardsForTopic(String topic, KafkaProducerOptions options) {
        try (Producer producer = new KafkaProducer(ImmutableMap.of(
            BOOTSTRAP_SERVERS_CONFIG, options.getKafkaBootstrapServers(),
            KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {

            return producer.partitionsFor(topic).size();
        }
    }

    public static PTransform<PBegin, PCollection<String>> readValuesAsString(
        KafkaConsumerOptions options,
        KafkaTopicsAndPartitionsCounts topicsAndPartitionsList) {

        KafkaIO.Read<Void, String> kafkaReader = createRead(options, topicsAndPartitionsList);
        return new KafkaValueReader(kafkaReader);
    }

    public static KafkaIO.Read<Void, String> createRead(KafkaConsumerOptions options,
                                                        KafkaTopicsAndPartitionsCounts topicAndPartitionCountList) {
        KafkaIO.Read<Void, String> read = KafkaIO.<Void, String>read()
            .withBootstrapServers(options.getKafkaBootstrapServers())
            .withProcessingTime()
            .withKeyDeserializer(VoidDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withReadCommitted()
            .commitOffsetsInFinalize()
            .updateConsumerProperties(new ImmutableMap.Builder<String, Object>()
                .put(ENABLE_AUTO_COMMIT_CONFIG, false)
                .put(AUTO_OFFSET_RESET_CONFIG, options.getKafkaConsumerOffsetReset())
                .put(GROUP_ID_CONFIG, options.getKafkaConsumerGroup())
                .build())
            .withConsumerFactoryFn(new ConsumerFactoryFn());

        return addTopic(read, topicAndPartitionCountList);
    }

    private static KafkaIO.Read<Void, String> addTopic(KafkaIO.Read<Void, String> kafkaReader,
                                                       KafkaTopicsAndPartitionsCounts topicAndPartitionCountList) {
        if (topicAndPartitionCountList.isPartitionsCountSpecifiedInAllTopics()) {
            return kafkaReader.withTopicPartitions(topicAndPartitionCountList.toTopicPartitions());
        } else {
            if (topicAndPartitionCountList.isPartitionsCountSpecifiedInAnyOfTheTopics()) {
                log.warn(
                    "Topic partitions is specified not in all topics, " +
                        "will disregard specified partitions: " + topicAndPartitionCountList);
            }
            return kafkaReader.withTopics(topicAndPartitionCountList.getTopicsNames());
        }
    }

    private static class ConsumerFactoryFn
        implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {
        public Consumer<byte[], byte[]> apply(Map<String, Object> config) {

            return new KafkaConsumer<>(new HashMap<>(config));
        }
    }

    private static class ProducerFactoryFn
        implements SerializableFunction<Map<String, Object>, Producer<Void, String>> {
        public Producer<Void, String> apply(Map<String, Object> config) {

            return new KafkaProducer<>(new HashMap<>(config));
        }
    }

}
