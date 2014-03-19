package com.hpcloud.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VerticaOutputProcessorConfiguration {

    @JsonProperty
    Integer batchSize;

    public Integer getBatchSize() {
        return batchSize;
    }
}
