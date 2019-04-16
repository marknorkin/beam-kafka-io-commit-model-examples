package com.marknorkin.beam.kafkaio.commit.model.examples.options;


import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface KafkaConsumerOptions extends KafkaOptions {

    @Description("The Kafka consumer group.")
    @Validation.Required
    String getKafkaConsumerGroup();
    void setKafkaConsumerGroup(String value);

    @Description("What to do when there is no initial offset in Kafka or if the current offset does not exist " +
        "any more on the server, available values: earliest or latest")
    @Validation.Required
    String getKafkaConsumerOffsetReset();
    void setKafkaConsumerOffsetReset(String value);

}
