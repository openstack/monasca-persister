/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package monasca.persister.repository;

import io.dropwizard.setup.Environment;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import monasca.persister.configuration.MonPersisterConfiguration;

import org.influxdb.dto.Serie;
import org.influxdb.dto.Serie.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.inject.Inject;
import com.hpcloud.mon.common.event.AlarmStateTransitionedEvent;

public class InfluxDBAlarmRepository extends InfluxRepository implements AlarmRepository {
  private static final Logger logger = LoggerFactory.getLogger(InfluxDBAlarmRepository.class);
  private static final String ALARM_STATE_HISTORY_NAME = "alarm_state_history";
  private static final String[] COLUMN_NAMES = {"tenant_id", "alarm_id", "metrics", "old_state",
      "new_state", "reason", "reason_data", "time"};
  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER
        .setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
  }

  protected final Timer flushTimer;

  private List<AlarmStateTransitionedEvent> alarmStateTransitionedEventList = new LinkedList<>();

  public final Meter alarmStateHistoryMeter;

  @Inject
  public InfluxDBAlarmRepository(MonPersisterConfiguration configuration, Environment environment) {
    super(configuration, environment);
    this.flushTimer =
        this.environment.metrics().timer(MetricRegistry.name(getClass(), "flush-timer"));
    this.alarmStateHistoryMeter =
        this.environment.metrics().meter(
            MetricRegistry.name(getClass(), "alarm_state_history-meter"));
  }

  @Override
  public void addToBatch(AlarmStateTransitionedEvent alarmStateTransitionedEvent) {
    alarmStateTransitionedEventList.add(alarmStateTransitionedEvent);
    this.alarmStateHistoryMeter.mark();
  }

  @Override
  public void flush() {
    try {
      if (this.alarmStateTransitionedEventList.isEmpty()) {
        logger.debug("There are no alarm state transition events to be written to the influxDB");
        logger.debug("Returning from flush");
        return;
      }

      long startTime = System.currentTimeMillis();
      Timer.Context context = flushTimer.time();

      final Builder builder = new Serie.Builder(ALARM_STATE_HISTORY_NAME);
      logger.debug("Created serie: {}", ALARM_STATE_HISTORY_NAME);

      builder.columns(COLUMN_NAMES);

      if (logger.isDebugEnabled()) {
        logColumnNames(COLUMN_NAMES);
      }

      for (AlarmStateTransitionedEvent alarmStateTransitionedEvent : alarmStateTransitionedEventList) {
        builder.values(alarmStateTransitionedEvent.tenantId, alarmStateTransitionedEvent.alarmId,
            OBJECT_MAPPER.writeValueAsString(alarmStateTransitionedEvent.metrics),
            alarmStateTransitionedEvent.oldState, alarmStateTransitionedEvent.newState,
            alarmStateTransitionedEvent.stateChangeReason, "{}",
            alarmStateTransitionedEvent.timestamp);
      }

      final Serie[] series = {builder.build()};

      if (logger.isDebugEnabled()) {
        logColValues(series[0]);
      }

      this.influxDB.write(this.configuration.getInfluxDBConfiguration().getName(),
          TimeUnit.SECONDS, series);

      context.stop();
      long endTime = System.currentTimeMillis();
      logger.debug("Commiting batch took {} seconds", (endTime - startTime) / 1000);

    } catch (Exception e) {
      logger.error("Failed to write alarm state history to database", e);
    }

    this.alarmStateTransitionedEventList.clear();
  }
}
