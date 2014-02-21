package com.hpcloud;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DisruptorConfiguration {

    @JsonProperty
    Integer bufferSize;

    @JsonProperty
    Integer numThreads;


}
