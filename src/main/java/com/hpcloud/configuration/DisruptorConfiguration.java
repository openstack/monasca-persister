package com.hpcloud.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DisruptorConfiguration {

    @JsonProperty
    Integer bufferSize;

    public Integer getBufferSize() {
        return bufferSize;
    }

    @JsonProperty
    Integer numProcessors;

    public Integer getNumProcessors() {
        return numProcessors;
    }

}
