package com.marknorkin.beam.directrunner.sample.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface KafkaRawMessagesTopicOptions extends PipelineOptions {

    @Description("Kafka topic name for raw messages.")
    @Validation.Required
    String getKafkaRawMessagesTopic();
    void setKafkaRawMessagesTopic(String value);

    @Description("Kafka partitions number for raw messages.")
    Integer getKafkaRawMessagesTopicPartitionNumber();
    void setKafkaRawMessagesTopicPartitionNumber(Integer value);
}
