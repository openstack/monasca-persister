package com.hpcloud.mon.persister.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VerticaMetricRepositoryConfiguration {

    @JsonProperty
    Integer maxCacheSize;

    public Integer getMaxCacheSize() {
        return maxCacheSize;
    }
}
