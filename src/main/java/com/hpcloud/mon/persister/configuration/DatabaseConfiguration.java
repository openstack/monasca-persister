package com.hpcloud.mon.persister.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DatabaseConfiguration {

    @JsonProperty
    String databaseType;

    public String getDatabaseType() { return databaseType; }
}
