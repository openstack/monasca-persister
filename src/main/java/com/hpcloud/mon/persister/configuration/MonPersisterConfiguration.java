package com.hpcloud.mon.persister.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.db.DatabaseConfiguration;

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
    private final KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();

    public KafkaConfiguration getKafkaConfiguration() {
        return kafkaConfiguration;
    }

    @Valid
    @NotNull
    @JsonProperty
    private final DisruptorConfiguration disruptorConfiguration = new DisruptorConfiguration();

    public DisruptorConfiguration getDisruptorConfiguration() {
        return disruptorConfiguration;
    }

    @Valid
    @NotNull
    @JsonProperty
    private final VerticaOutputProcessorConfiguration verticaOutputProcessorConfiguration = new VerticaOutputProcessorConfiguration();

    public VerticaOutputProcessorConfiguration getVerticaOutputProcessorConfiguration() {
        return verticaOutputProcessorConfiguration;
    }

    @Valid
    @NotNull
    @JsonProperty
    private final DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration();

    public DatabaseConfiguration getDatabaseConfiguration() {
        return databaseConfiguration;
    }

    @Valid
    @NotNull
    @JsonProperty
    private final DeduperConfiguration monDeDuperConfiguration = new DeduperConfiguration();

    public DeduperConfiguration getMonDeDuperConfiguration() {
        return monDeDuperConfiguration;
    }

    @Valid
    @NotNull
    @JsonProperty
    private final VerticaMetricRepositoryConfiguration verticaMetricRepositoryConfiguration = new VerticaMetricRepositoryConfiguration();

    public VerticaMetricRepositoryConfiguration getVerticaMetricRepositoryConfiguration() {
        return verticaMetricRepositoryConfiguration;
    }
}
