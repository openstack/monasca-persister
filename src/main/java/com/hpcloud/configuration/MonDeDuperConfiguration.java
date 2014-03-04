package com.hpcloud.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MonDeDuperConfiguration {

    @JsonProperty
    Integer dedupeRunFrequencySeconds;

    public Integer getDedupeRunFrequencySeconds() {
        return dedupeRunFrequencySeconds;
    }
}
