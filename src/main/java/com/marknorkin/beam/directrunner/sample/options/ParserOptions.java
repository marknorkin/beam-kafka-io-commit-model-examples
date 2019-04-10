package com.marknorkin.beam.directrunner.sample.options;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

public interface ParserOptions extends PipelineOptions,
        StreamingOptions,
        KafkaConsumerOptions,
        KafkaProducerOptions,
        KafkaRawMessagesTopicOptions,
        KafkaParsedEventsTopicOptions,
        KafkaPartiallyParsedEventsTopicOptions,
        KafkaUnknownMessagesTopicOptions {

}

