package com.marknorkin.beam.directrunner.sample;

import com.marknorkin.beam.directrunner.sample.domain.ParsedEventDto;
import com.marknorkin.beam.directrunner.sample.domain.RawEventDto;
import com.marknorkin.beam.directrunner.sample.options.KafkaConsumerOptions;
import com.marknorkin.beam.directrunner.sample.options.ParserOptions;
import com.marknorkin.beam.directrunner.sample.serialization.JsonDeserializer;
import com.marknorkin.beam.directrunner.sample.serialization.JsonSerializer;
import com.marknorkin.beam.directrunner.sample.support.KafkaTopicsAndPartitionsCounts;
import com.marknorkin.beam.directrunner.sample.transform.ParseTransform;
import com.marknorkin.beam.directrunner.sample.transform.IsEventKnownTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;

public final class ParserFlow implements Serializable {
    public static final String PARSED_EVENTS_SINK_GROUP_ID = "parsedEventsSinkGroupId";
    public static final String PARTIALLY_PARSED_EVENTS_SINK_GROUP_ID = "partiallyParsedEventsSinkGroupId";
    public static final String UNKNOWN_MESSAGES_SINK_GROUP_ID = "unknownMessagesSinkGroupId";

    static PipelineResult run(ParserOptions options) {

        PTransform<PBegin, PCollection<String>> readInputValues = KafkaIOConfig.readValuesAsString(
            options.as(KafkaConsumerOptions.class),
            createTopicsAndPartitionsCounts(options));

        return new ParserFlow().run(Pipeline.create(options),
            readInputValues,
            KafkaIOConfig.write(
                options,
                options.getKafkaParsedEventsTopic(),
                PARSED_EVENTS_SINK_GROUP_ID,
                options.getKafkaParsedEventsTopicPartitionNumber()),
            KafkaIOConfig.write(
                options,
                options.getKafkaPartiallyParsedEventsTopic(),
                PARTIALLY_PARSED_EVENTS_SINK_GROUP_ID,
                options.getKafkaPartiallyParsedEventsTopicPartitionNumber()),
            KafkaIOConfig.write(
                options,
                options.getKafkaUnknownMessagesTopic(),
                UNKNOWN_MESSAGES_SINK_GROUP_ID,
                options.getKafkaUnknownMessagesTopicPartitionNumber()));
    }

    private static KafkaTopicsAndPartitionsCounts createTopicsAndPartitionsCounts(ParserOptions options) {
        return new KafkaTopicsAndPartitionsCounts(
            options.getKafkaRawMessagesTopic(),
            options.getKafkaRawMessagesTopicPartitionNumber());
    }

    PipelineResult run(Pipeline pipeline,
                       PTransform<PBegin, PCollection<String>> input,
                       PTransform<PCollection<String>, PDone> parsedEventsOutput,
                       PTransform<PCollection<String>, PDone> partiallyParsedEventsOutput,
                       PTransform<PCollection<String>, PDone> unknownMessagesOutput) {

        final TupleTag<RawEventDto> knownEventTag = new TupleTag<RawEventDto>() { };
        final TupleTag<RawEventDto> unknownEventTag = new TupleTag<RawEventDto>() { };
        final TupleTag<ParsedEventDto> parsedEventTag = new TupleTag<ParsedEventDto>() { };
        final TupleTag<ParsedEventDto> partiallyParsedEventTag = new TupleTag<ParsedEventDto>() { };

        PCollectionTuple byDatasourceTypes = pipeline
            .apply("Read raw messages", input)
            .apply("Deserialize raw messages", ParDo.of(new JsonDeserializer<>(RawEventDto.class)))
            .apply("Resolve message is known or not", ParDo.of( new IsEventKnownTransform(unknownEventTag)).withOutputTags(knownEventTag, TupleTagList.of(unknownEventTag)));

        PCollectionTuple allParsedEvents = byDatasourceTypes.get(knownEventTag)
            .apply("Parse and normalize event", ParDo.of(new ParseTransform(partiallyParsedEventTag)).withOutputTags(parsedEventTag, TupleTagList.of(partiallyParsedEventTag)));

        allParsedEvents.get(parsedEventTag)
            .apply("Serialize successfully parsed events", ParDo.of(new JsonSerializer<>()))
            .apply("Write successfully parsed event to Kafka", parsedEventsOutput);

        allParsedEvents.get(partiallyParsedEventTag)
            .apply("Serialize partially parsed events", ParDo.of(new JsonSerializer<>()))
            .apply("Write partially parsed event to Kafka", partiallyParsedEventsOutput);

        byDatasourceTypes.get(unknownEventTag)
            .apply("Serialize to JSON to write to unknown messages topic", ParDo.of(new JsonSerializer<>()))
            .apply("Write to Kafka unknown messages topic", unknownMessagesOutput);

        return pipeline.run();
    }
}
