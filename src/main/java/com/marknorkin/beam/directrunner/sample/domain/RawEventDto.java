package com.marknorkin.beam.directrunner.sample.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RawEventDto implements Serializable {
    private static final long serialVersionUID = 0;
    public static final String RAW_DATA = "data";
    public static final String UUID = "uuid";

    @JsonProperty(RAW_DATA)
    private final String data;
    @JsonProperty(UUID)
    private final String uuid;

    @JsonCreator
    public RawEventDto(
        @JsonProperty(value = RAW_DATA, required = true) String data,
        @JsonProperty(value = UUID, required = true) String uuid) {
        this.data = data;
        this.uuid = uuid;
    }
}
