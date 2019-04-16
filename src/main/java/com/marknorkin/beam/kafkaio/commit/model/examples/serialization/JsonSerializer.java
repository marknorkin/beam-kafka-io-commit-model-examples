package com.marknorkin.beam.kafkaio.commit.model.examples.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.io.Serializable;

@RequiredArgsConstructor
public class JsonSerializer<T extends Serializable> extends DoFn<T, String> {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @ProcessElement
    public void processElement(@Element T data, OutputReceiver<String> output) throws IOException {
        output.output(OBJECT_MAPPER.writeValueAsString(data));
    }
}
