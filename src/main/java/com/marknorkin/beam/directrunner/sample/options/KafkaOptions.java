package com.marknorkin.beam.directrunner.sample.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface KafkaOptions extends PipelineOptions {
    @Description("The kafka bootstrap servers")
    @Validation.Required
    String getKafkaBootstrapServers();
    void setKafkaBootstrapServers(String value);
}
