package com.marknorkin.beam.kafkaio.commit.model.examples.transform;

import com.marknorkin.beam.kafkaio.commit.model.examples.domain.ParsedEventDto;
import com.marknorkin.beam.kafkaio.commit.model.examples.domain.RawEventDto;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class ParseTransform extends DoFn<RawEventDto, ParsedEventDto> {

    public static final String EVENT_KNOWN_PARTIAL_MARKER = "PARTIAL";
    private final TupleTag<ParsedEventDto> partiallyParsedEventTag;

    public ParseTransform(TupleTag<ParsedEventDto> partiallyParsedEventTag) {
        this.partiallyParsedEventTag = partiallyParsedEventTag;
    }

    @ProcessElement
    public void process(ProcessContext processContext) {
        RawEventDto element = processContext.element();

        if (element.getData().contains(EVENT_KNOWN_PARTIAL_MARKER)) {
            processContext.output(partiallyParsedEventTag, new ParsedEventDto(element));
        } else {
            processContext.output(new ParsedEventDto(element));
        }
    }
}
