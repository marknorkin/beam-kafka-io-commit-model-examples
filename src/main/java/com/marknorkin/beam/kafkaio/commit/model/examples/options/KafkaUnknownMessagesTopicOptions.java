package com.marknorkin.beam.kafkaio.commit.model.examples.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface KafkaUnknownMessagesTopicOptions extends PipelineOptions {

    @Description("Kafka topic name for unknown messages.")
    @Validation.Required
    String getKafkaUnknownMessagesTopic();
    void setKafkaUnknownMessagesTopic(String value);

    @Description("Kafka partitions number for unknown messages topic.")
    Integer getKafkaUnknownMessagesTopicPartitionNumber();
    void setKafkaUnknownMessagesTopicPartitionNumber(Integer value);
}
