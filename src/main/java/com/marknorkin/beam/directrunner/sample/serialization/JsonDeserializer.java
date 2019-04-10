package com.marknorkin.beam.directrunner.sample.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.io.Serializable;

@Slf4j
public class JsonDeserializer<T extends Serializable> extends DoFn<String, T> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Class<T> clazz;

    public JsonDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @ProcessElement
    public void processElement(@Element String msg, OutputReceiver<T> output) {
        try {
            T syslogDto = OBJECT_MAPPER.readValue(msg, clazz);
            output.output(syslogDto);
        } catch (IOException e) {
            log.warn("Cannot parse event: {}", msg);
        }
    }
}
