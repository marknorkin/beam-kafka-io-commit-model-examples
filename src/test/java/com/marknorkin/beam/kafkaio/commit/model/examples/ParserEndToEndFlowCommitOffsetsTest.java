package com.marknorkin.beam.kafkaio.commit.model.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.marknorkin.beam.kafkaio.commit.model.examples.domain.RawEventDto;
import com.marknorkin.beam.kafkaio.commit.model.examples.options.ParserOptions;
import com.marknorkin.beam.kafkaio.commit.model.examples.support.KafkaConsumerOffsetsReader;
import com.marknorkin.beam.kafkaio.commit.model.examples.support.VoidDeserializer;
import com.marknorkin.beam.kafkaio.commit.model.examples.support.VoidSerializer;
import com.marknorkin.beam.kafkaio.commit.model.examples.transform.IsEventKnownTransform;
import com.marknorkin.beam.kafkaio.commit.model.examples.transform.ParseTransform;
import lombok.SneakyThrows;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Test is flaky as Direct Runner loses Kafka checkpoint marks offsets
 * https://jira.apache.org/jira/browse/BEAM-7047
 */
public class ParserEndToEndFlowCommitOffsetsTest implements Serializable {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String RAW_TOPIC = "raw_topic";
    private static final String PARSED_TOPIC = "parsed_topic";
    private static final String PARTIALLY_PARSED_TOPIC = "partially_parsed_topic";
    private static final String UNKNOWN_MESSAGES_TOPIC = "unknown_messages_topic";

    private static final String CONSUMER_GROUP_ID = "parser_consumer_group";
    private static final String EARLIEST_OFFSET_RESET = "earliest";

    /**
     * {@link org.apache.beam.runners.direct.UnboundedReadEvaluatorFactory.UnboundedReadEvaluator} ARBITRARY_MAX_ELEMENTS
     */
    private static final long DIRECT_RUNNER_COMMIT_MSG_INTERVAL = 10L;
    private String kafkaContainerBootstrapServers;
    private transient ParserOptions options;
    private transient Producer<Long, String> producer;
    private KafkaConsumerOffsetsReader kafkaConsumerOffsetsReader;

    @Rule
    transient public EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(useDefaults());

    @Before
    public void setUp() throws InterruptedException {
        kafkaContainerBootstrapServers = cluster.getBrokerList();

        options = PipelineOptionsFactory.fromArgs("--blockOnRun=false").as(ParserOptions.class);
        options.setKafkaBootstrapServers(kafkaContainerBootstrapServers);
        options.setKafkaConsumerOffsetReset(EARLIEST_OFFSET_RESET);
        options.setKafkaRawMessagesTopic(RAW_TOPIC);
        options.setKafkaRawMessagesTopicPartitionNumber(3);
        options.setKafkaParsedEventsTopic(PARSED_TOPIC);
        options.setKafkaPartiallyParsedEventsTopic(PARTIALLY_PARSED_TOPIC);
        options.setKafkaUnknownMessagesTopic(UNKNOWN_MESSAGES_TOPIC);
        options.setKafkaConsumerGroup(CONSUMER_GROUP_ID);
        options.setStreaming(true);

        PipelineOptionsValidator.validate(ParserOptions.class, options);

        producer = createProducer(kafkaContainerBootstrapServers);

        cluster.createTopic(TopicConfig.forTopic(RAW_TOPIC)
                .withNumberOfPartitions(3)
                .build());

        kafkaConsumerOffsetsReader = new KafkaConsumerOffsetsReader(kafkaContainerBootstrapServers);

        delay(5);
    }

    @After
    public void cleanUp() {
        producer.close();
        kafkaConsumerOffsetsReader.close();
    }

    @Test
    public void shouldTestOffsetCommit() throws InterruptedException, IOException {
        // Given
        // Construct and send messages to the input Kafka topic
        IntStream.range(0, 7).forEach(i -> sendEventToInputTopicPartition(createParsedEvent(i), 0));
        IntStream.range(7, 14).forEach(i -> sendEventToInputTopicPartition(createParsedEvent(i), 1));
        IntStream.range(14, 21).forEach(i -> sendEventToInputTopicPartition(createParsedEvent(i), 2));

        IntStream.range(21, 23).forEach(i -> sendEventToInputTopicPartition(createPartiallyParsedEvent(i), 0));
        IntStream.range(23, 25).forEach(i -> sendEventToInputTopicPartition(createPartiallyParsedEvent(i), 1));
        IntStream.range(25, 27).forEach(i -> sendEventToInputTopicPartition(createPartiallyParsedEvent(i), 2));

        IntStream.range(27, 29).forEach(i -> sendEventToInputTopicPartition(createUnknownEvent(i), 0));
        IntStream.range(29, 31).forEach(i -> sendEventToInputTopicPartition(createUnknownEvent(i), 1));
        IntStream.range(31, 33).forEach(i -> sendEventToInputTopicPartition(createUnknownEvent(i), 2));
        producer.flush();
        Map<TopicPartition, Long> expectedCommittedOffsets = ImmutableMap.of(
                new TopicPartition(RAW_TOPIC, 0), DIRECT_RUNNER_COMMIT_MSG_INTERVAL,
                new TopicPartition(RAW_TOPIC, 1), DIRECT_RUNNER_COMMIT_MSG_INTERVAL,
                new TopicPartition(RAW_TOPIC, 2), DIRECT_RUNNER_COMMIT_MSG_INTERVAL);

        // When
        // Start the first flow
        PipelineResult pipelineResult = ParserFlowWithKafkaManualCommit.run(options);

        // Then
        await("sent raw messages are read and offsets are committed")
                .atMost(3, MINUTES)
                .with()
                .ignoreExceptions()
                .and()
                .pollInterval(100, MILLISECONDS)
                .until(() -> kafkaConsumerOffsetsReader.getConsumerGroupTopicPartitionOffsets(CONSUMER_GROUP_ID), equalTo(expectedCommittedOffsets));

        pipelineResult.cancel();

        // Verify that messages sent above were processed properly
        List<ConsumerRecord<Void, String>> parsedEvents = readEventsFromTopic(PARSED_TOPIC);
        List<ConsumerRecord<Void, String>> partiallyParsedEvents = readEventsFromTopic(PARTIALLY_PARSED_TOPIC);
        List<ConsumerRecord<Void, String>> unknownEvents = readEventsFromTopic(UNKNOWN_MESSAGES_TOPIC);

        assertThat(parsedEvents).hasSize(21);
        IntStream.range(0, 21).forEach(id -> assertThat(isEventPresent(id, parsedEvents)).isTrue());

        assertThat(partiallyParsedEvents).hasSize(6);
        IntStream.range(21, 27).forEach(id -> assertThat(isEventPresent(id, partiallyParsedEvents)).isTrue());

        assertThat(unknownEvents).hasSize(6);
        IntStream.range(27, 33).forEach(id -> assertThat(isEventPresent(id, unknownEvents)).isTrue());

        // Send new message to the input topic
        RawEventDto parsedEvent = createParsedEvent(33);
        sendEventToInputTopicPartition(parsedEvent, 0);

        // Start the second flow
        PipelineResult pipelineResult2 = ParserFlowWithKafkaManualCommit.run(options);
        pipelineResult2.waitUntilFinish(Duration.standardMinutes(1));

        // Verify that old messages with committed offset were not re-read and new message was processed properly
        List<ConsumerRecord<Void, String>> parsedEvents2 = readEventsFromTopic(PARSED_TOPIC);

        assertThat(parsedEvents2).hasSize(1);
        assertThat(isEventPresent(parsedEvent.getUuid(), parsedEvents2)).isTrue();

        assertThat(readEventsFromTopic(PARTIALLY_PARSED_TOPIC)).isEmpty();
        assertThat(readEventsFromTopic(UNKNOWN_MESSAGES_TOPIC)).hasSize(3);
    }

    private RawEventDto createParsedEvent(int id) {
        return RawEventDto.builder()
                .data(IsEventKnownTransform.EVENT_KNOWN_MARKER)
                .uuid("UUID" + id)
                .build();
    }

    private RawEventDto createUnknownEvent(int id) {
        return RawEventDto.builder()
                .data("@#$%^&")
                .uuid("UUID" + id)
                .build();
    }

    private RawEventDto createPartiallyParsedEvent(int id) {
        return RawEventDto.builder()
                .data(IsEventKnownTransform.EVENT_KNOWN_MARKER + " " + ParseTransform.EVENT_KNOWN_PARTIAL_MARKER)
                .uuid("UUID" + id)
                .build();
    }

    private boolean isEventPresent(String id, List<ConsumerRecord<Void, String>> events) {
        return events.stream().anyMatch(event -> event.value().contains(id));
    }

    private boolean isEventPresent(int id, List<ConsumerRecord<Void, String>> events) {
        return isEventPresent("UUID" + id, events);
    }

    @SneakyThrows
    private void sendEventToInputTopicPartition(RawEventDto event, int partition) {
        producer.send(new ProducerRecord<>(RAW_TOPIC, partition, null, OBJECT_MAPPER.writeValueAsString(event)));
    }

    private List<ConsumerRecord<Void, String>> readEventsFromTopic(String topic) {
        try (Consumer<Void, String> consumer = createConsumer(kafkaContainerBootstrapServers)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<Void, String> consumerRecords = consumer.poll(java.time.Duration.ofMillis(1000));
            Iterable<ConsumerRecord<Void, String>> recordsIter = consumerRecords.records(topic);
            List<ConsumerRecord<Void, String>> records = StreamSupport.stream(recordsIter.spliterator(), false).collect(Collectors.toList());
            consumer.commitSync();
            return records;
        }
    }

    private static Consumer<Void, String> createConsumer(String bootstrapServers) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST_OFFSET_RESET);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);

        return new KafkaConsumer<>(props);
    }

    private static Producer<Long, String> createProducer(String bootstrapServers) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test_client_id");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

}

