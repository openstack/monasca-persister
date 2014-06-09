/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hpcloud.mon.persister.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;

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
    private final DataSourceFactory dataSourceFactory = new DataSourceFactory();

    public DataSourceFactory getDataSourceFactory() {
        return dataSourceFactory;
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

    @Valid
    @NotNull
    @JsonProperty
    private final DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration();

    public DatabaseConfiguration getDatabaseConfiguration () {
        return databaseConfiguration;
    }

    @Valid
    @NotNull
    @JsonProperty
    private final InfluxDBConfiguration influxDBConfiguration = new InfluxDBConfiguration();

    public InfluxDBConfiguration getInfluxDBConfiguration() {
        return influxDBConfiguration;
    }
}
