package com.marknorkin.beam.kafkaio.commit.model.examples.transform;

import com.marknorkin.beam.kafkaio.commit.model.examples.domain.RawEventDto;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class IsEventKnownTransform extends DoFn<RawEventDto, RawEventDto> {

    public static final String EVENT_KNOWN_MARKER = "KNOWN";
    private final TupleTag<RawEventDto> unknownEventTag;

    public IsEventKnownTransform(TupleTag<RawEventDto> unknownEventTag) {
        this.unknownEventTag = unknownEventTag;
    }

    @ProcessElement
    public void process(ProcessContext processContext) {
        RawEventDto element = processContext.element();

        if (element.getData().contains(EVENT_KNOWN_MARKER)) {
            processContext.output(element);
        } else {
            processContext.output(unknownEventTag, element);
        }
    }
}
