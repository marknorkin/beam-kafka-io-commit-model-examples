package com.marknorkin.beam.kafkaio.commit.model.examples.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface KafkaParsedEventsTopicOptions extends PipelineOptions {

    @Description("Kafka topic name for successfully parsed events.")
    @Validation.Required
    String getKafkaParsedEventsTopic();
    void setKafkaParsedEventsTopic(String value);

    @Description("Kafka partitions number for parsed events topic.")
    Integer getKafkaParsedEventsTopicPartitionNumber();
    void setKafkaParsedEventsTopicPartitionNumber(Integer value);
}
