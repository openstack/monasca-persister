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

package monasca.persister.repository.influxdb;

import monasca.common.model.event.AlarmStateTransitionedEvent;
import monasca.persister.repository.AlarmRepo;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import io.dropwizard.setup.Environment;

public abstract class InfluxAlarmRepo implements AlarmRepo {

  private static final Logger logger = LoggerFactory.getLogger(InfluxAlarmRepo.class);

  protected static final String ALARM_STATE_HISTORY_NAME = "alarm_state_history";

  public final Timer flushTimer;
  public final Meter alarmStateHistoryMeter;

  protected List<AlarmStateTransitionedEvent> alarmStateTransitionedEventList = new LinkedList<>();

  public InfluxAlarmRepo(final Environment env) {

    this.flushTimer =
        env.metrics().timer(MetricRegistry.name(getClass(), "flush-timer"));

    this.alarmStateHistoryMeter =
        env.metrics().meter(
            MetricRegistry.name(getClass(), "alarm_state_history-meter"));
  }

  protected abstract void write (String id) throws Exception;

  @Override
  public void addToBatch(AlarmStateTransitionedEvent alarmStateTransitionedEvent) {

    this.alarmStateTransitionedEventList.add(alarmStateTransitionedEvent);

    this.alarmStateHistoryMeter.mark();
  }

  @Override
  public void flush(String id) {
    try {

      if (this.alarmStateTransitionedEventList.isEmpty()) {
        logger.debug("[{}]: no alarm state transition msg to be written to the influxDB", id);
        logger.debug("[{}]: returning from flush", id);
        return;
      }

      long startTime = System.currentTimeMillis();
      Timer.Context context = flushTimer.time();

      write(id);

      context.stop();
      long endTime = System.currentTimeMillis();
      logger.debug("[{}]: flushing batch took {} seconds", id, (endTime - startTime) / 1000);

    } catch (Exception e) {
      logger.error("[{}]: failed to write alarm state history to database", id, e);
    }

    this.alarmStateTransitionedEventList.clear();
  }
}
