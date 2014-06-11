package com.hpcloud.mon.persister.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MetricConfiguration {

    @JsonProperty
    String topic;

    public String getTopic() {
        return topic;
    }
}
