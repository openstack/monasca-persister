/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
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

package monasca.persister.repository;

import com.google.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import org.influxdb.dto.Serie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import io.dropwizard.setup.Environment;
import monasca.common.model.event.AlarmStateTransitionedEvent;

public class InfluxV8AlarmRepo extends InfluxAlarmRepo {

  private static final Logger logger = LoggerFactory.getLogger(InfluxV8AlarmRepo.class);

  private static final String[]
      COLUMN_NAMES =
      {"tenant_id", "alarm_id", "metrics", "old_state", "new_state", "sub_alarms", "reason", "reason_data",
       "time"};

  private final InfluxV8RepoWriter influxV8RepoWriter;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Inject
  public InfluxV8AlarmRepo(final Environment env,
                           final InfluxV8RepoWriter influxV8RepoWriter) {

    super(env);
    this.influxV8RepoWriter = influxV8RepoWriter;

    this.objectMapper.setPropertyNamingStrategy(
        PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

  }

  @Override
  protected void write() throws JsonProcessingException {

    final Serie.Builder builder = new Serie.Builder(ALARM_STATE_HISTORY_NAME);
    logger.debug("Created serie: {}", ALARM_STATE_HISTORY_NAME);

    builder.columns(COLUMN_NAMES);

    if (logger.isDebugEnabled()) {
      this.influxV8RepoWriter.logColumnNames(COLUMN_NAMES);
    }

    for (AlarmStateTransitionedEvent alarmStateTransitionedEvent : this.alarmStateTransitionedEventList) {
      builder.values(alarmStateTransitionedEvent.tenantId, alarmStateTransitionedEvent.alarmId,
                     this.objectMapper.writeValueAsString(alarmStateTransitionedEvent.metrics),
                     alarmStateTransitionedEvent.oldState, alarmStateTransitionedEvent.newState,
                     this.objectMapper.writeValueAsString(alarmStateTransitionedEvent.subAlarms),
                     alarmStateTransitionedEvent.stateChangeReason, "{}",
                     alarmStateTransitionedEvent.timestamp);
    }

    final Serie[] series = {builder.build()};

    if (logger.isDebugEnabled()) {
      this.influxV8RepoWriter.logColValues(series[0]);
    }

    this.influxV8RepoWriter.write(TimeUnit.SECONDS, series);
  }

}
