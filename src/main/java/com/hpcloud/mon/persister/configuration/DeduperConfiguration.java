package com.hpcloud.mon.persister.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DeduperConfiguration {

    @JsonProperty
    Integer dedupeRunFrequencySeconds;

    public Integer getDedupeRunFrequencySeconds() {
        return dedupeRunFrequencySeconds;
    }
}
