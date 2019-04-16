package com.marknorkin.beam.kafkaio.commit.model.examples.support;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class KafkaValueReader extends PTransform<PBegin, PCollection<String>> {
    private final KafkaIO.Read<Void, String> reader;

    public KafkaValueReader(KafkaIO.Read<Void, String> reader) {
        this.reader = reader;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
        return input
            .apply(reader)
            .apply(MapElements
                .into(TypeDescriptor.of(String.class))
                .via(record -> record.getKV().getValue())
            );
    }
}
