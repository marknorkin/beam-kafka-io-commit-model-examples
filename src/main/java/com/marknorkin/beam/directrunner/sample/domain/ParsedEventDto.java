package com.marknorkin.beam.directrunner.sample.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
public class ParsedEventDto implements Serializable {
    private static final long serialVersionUID = 0;

    public static final String UUID = "uuid";
    public static final String RAW_DATA = "rawData";

    @NonNull
    @JsonProperty(UUID)
    private String uuid;
    @NonNull
    @JsonProperty(RAW_DATA)
    private String rawData;

    public ParsedEventDto(RawEventDto rawEventDto) {
        this(rawEventDto.getData(), rawEventDto.getUuid());
    }

    private ParsedEventDto(String data, String uuid) {
        setRawData(data);
        setUuid(uuid);
    }

}
