package com.hpcloud.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VerticaMetricRepositoryConfiguration {

    @JsonProperty
    Integer maxCacheSize;

    public Integer getMaxCacheSize() {
        return maxCacheSize;
    }
}
