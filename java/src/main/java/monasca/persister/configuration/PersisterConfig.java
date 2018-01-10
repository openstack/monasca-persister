/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Copyright (c) 2017 SUSE LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monasca.persister.configuration;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import monasca.common.configuration.CassandraDbConfiguration;
import monasca.common.configuration.DatabaseConfiguration;
import monasca.common.configuration.InfluxDbConfiguration;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@JsonIgnoreProperties(ignoreUnknown=true)
public class PersisterConfig extends Configuration {

  @JsonProperty
  private String name;
  private String _name = "monasca-persister";

  public String getName() {
    if ( name == null ) {
      return _name;
    }
    return name;
  }

  @JsonProperty
  @NotNull
  @Valid
  private final PipelineConfig alarmHistoryConfiguration = new PipelineConfig();

  public PipelineConfig getAlarmHistoryConfiguration() {
    // Set alarm history configuration specific defaults
    alarmHistoryConfiguration.setDefaults("alarm-state-transitions",
                               "1_alarm-state-transitions",
                               1);
    return alarmHistoryConfiguration;
  }

  @JsonProperty
  @NotNull
  @Valid
  private final PipelineConfig metricConfiguration = new PipelineConfig();


  public PipelineConfig getMetricConfiguration() {
    // Set metric configuration specific defaults
    metricConfiguration.setDefaults("metrics",
                                    "1_metrics",
                                    20000);
    return metricConfiguration;
  }

  @Valid
  @NotNull
  @JsonProperty
  private final KafkaConfig kafkaConfig = new KafkaConfig();

  public KafkaConfig getKafkaConfig() {
    return kafkaConfig;
  }

  @JsonProperty
  private final DataSourceFactory dataSourceFactory = new DataSourceFactory();

  public DataSourceFactory getDataSourceFactory() {
    return dataSourceFactory;
  }

  @Valid
  @NotNull
  @JsonProperty
  private final VerticaMetricRepoConfig verticaMetricRepoConfig =
      new VerticaMetricRepoConfig();

  public VerticaMetricRepoConfig getVerticaMetricRepoConfig() {
    return verticaMetricRepoConfig;
  }

  @Valid
  @NotNull
  @JsonProperty
  private final DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration();

  public DatabaseConfiguration getDatabaseConfiguration() {
    return databaseConfiguration;
  }

  @Valid
  @JsonProperty
  private final InfluxDbConfiguration influxDbConfiguration = new InfluxDbConfiguration();

  public InfluxDbConfiguration getInfluxDBConfiguration() {
    return influxDbConfiguration;
  }

  @Valid
  @JsonProperty
  private final CassandraDbConfiguration cassandraDbConfiguration = new CassandraDbConfiguration();

  public CassandraDbConfiguration getCassandraDbConfiguration() {
    return cassandraDbConfiguration;
  }
}
