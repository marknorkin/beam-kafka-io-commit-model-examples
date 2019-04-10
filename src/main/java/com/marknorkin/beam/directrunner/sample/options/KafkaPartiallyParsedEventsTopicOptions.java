package com.marknorkin.beam.directrunner.sample.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface KafkaPartiallyParsedEventsTopicOptions extends PipelineOptions {

    @Description("Kafka topic name for partially parsed")
    @Validation.Required
    String getKafkaPartiallyParsedEventsTopic();
    void setKafkaPartiallyParsedEventsTopic(String value);

    @Description("Kafka partitions number for partially parsed events topic.")
    Integer getKafkaPartiallyParsedEventsTopicPartitionNumber();
    void setKafkaPartiallyParsedEventsTopicPartitionNumber(Integer value);
}
