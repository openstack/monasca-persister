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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.LinkedList;
import java.util.List;

import io.dropwizard.setup.Environment;

public abstract class InfluxAlarmRepo
    extends InfluxRepo<AlarmStateTransitionedEvent> {

  protected static final String ALARM_STATE_HISTORY_NAME = "alarm_state_history";

  protected final Meter alarmStateHistoryMeter;

  protected List<AlarmStateTransitionedEvent> alarmStateTransitionedEventList = new LinkedList<>();

  public InfluxAlarmRepo(final Environment env) {

    super(env);

    this.alarmStateHistoryMeter =
        env.metrics().meter(
            MetricRegistry.name(getClass(), "alarm_state_history-meter"));
  }

  @Override
  public void addToBatch(AlarmStateTransitionedEvent alarmStateTransitionedEvent, String id) {

    this.alarmStateTransitionedEventList.add(alarmStateTransitionedEvent);

    this.alarmStateHistoryMeter.mark();
  }

  @Override
  protected void clearBuffers() {

    this.alarmStateTransitionedEventList.clear();

  }

  @Override
  protected boolean isBufferEmpty() {

    return this.alarmStateTransitionedEventList.isEmpty();

  }

}
