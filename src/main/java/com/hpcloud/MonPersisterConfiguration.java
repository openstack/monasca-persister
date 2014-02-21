package com.hpcloud;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.config.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class MonPersisterConfiguration extends Configuration {

    @JsonProperty
    private String name;

    public String getName() {
        return name;
    }

    @Valid
    @NotNull
    @JsonProperty
    private KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();

    public KafkaConfiguration getKafkaConfiguration() {
        return kafkaConfiguration;
    }

    @Valid
    @NotNull
    @JsonProperty
    private DisruptorConfiguration disruptorConfiguration = new DisruptorConfiguration();

    public DisruptorConfiguration getDisruptorConfiguration() {
        return disruptorConfiguration;
    }

    @Valid
    @NotNull
    @JsonProperty
    private VerticaOutputProcessorConfiguration verticaOutputProcessorConfiguration = new VerticaOutputProcessorConfiguration();

    public VerticaOutputProcessorConfiguration getVerticaOutputProcessorConfiguration() {
        return verticaOutputProcessorConfiguration;
    }
}
