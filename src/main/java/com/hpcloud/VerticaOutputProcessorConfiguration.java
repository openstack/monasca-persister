package com.hpcloud;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VerticaOutputProcessorConfiguration {

    @JsonProperty
    Integer batchSize;

    @JsonProperty
    Integer numProcessors;
}
