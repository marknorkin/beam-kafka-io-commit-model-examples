package com.marknorkin.beam.kafkaio.commit.model.examples;

import com.marknorkin.beam.kafkaio.commit.model.examples.domain.ParsedEventDto;
import com.marknorkin.beam.kafkaio.commit.model.examples.domain.RawEventDto;
import com.marknorkin.beam.kafkaio.commit.model.examples.options.KafkaConsumerOptions;
import com.marknorkin.beam.kafkaio.commit.model.examples.options.ParserOptions;
import com.marknorkin.beam.kafkaio.commit.model.examples.serialization.JsonDeserializer;
import com.marknorkin.beam.kafkaio.commit.model.examples.serialization.JsonSerializer;
import com.marknorkin.beam.kafkaio.commit.model.examples.support.KafkaTopicsAndPartitionsCounts;
import com.marknorkin.beam.kafkaio.commit.model.examples.transform.IsEventKnownTransform;
import com.marknorkin.beam.kafkaio.commit.model.examples.transform.ParseTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;

public final class ParserFlowWithKafkaAutoCommit implements Serializable {

    static PipelineResult run(ParserOptions options) {

        PTransform<PBegin, PCollection<String>> readInputValues = KafkaIOConfig.readAutoCommittedValuesAsString(
            options.as(KafkaConsumerOptions.class),
            createTopicsAndPartitionsCounts(options));

        return new ParserFlowWithKafkaAutoCommit().run(Pipeline.create(options),
            readInputValues,
            KafkaIOConfig.writeWithoutEOS(
                options,
                options.getKafkaParsedEventsTopic()),
            KafkaIOConfig.writeWithoutEOS(
                options,
                options.getKafkaPartiallyParsedEventsTopic()),
            KafkaIOConfig.writeWithoutEOS(
                options,
                options.getKafkaUnknownMessagesTopic()));
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
