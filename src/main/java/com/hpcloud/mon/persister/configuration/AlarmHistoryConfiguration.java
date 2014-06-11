package com.hpcloud.mon.persister.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AlarmHistoryConfiguration {

    @JsonProperty
    String topic;

    public String getTopic() {
        return topic;
    }
}
