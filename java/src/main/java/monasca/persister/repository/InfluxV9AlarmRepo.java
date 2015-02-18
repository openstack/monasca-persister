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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.dropwizard.setup.Environment;
import monasca.common.model.event.AlarmStateTransitionedEvent;
import monasca.persister.repository.influxdb.InfluxPoint;

public class InfluxV9AlarmRepo extends InfluxAlarmRepo {

  private static final Logger logger = LoggerFactory.getLogger(InfluxV9AlarmRepo.class);

  private final InfluxV9RepoWriter influxV9RepoWriter;

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTime();

  @Inject
  public InfluxV9AlarmRepo(final Environment env,
                           final InfluxV9RepoWriter influxV9RepoWriter) {

    super(env);
    this.influxV9RepoWriter = influxV9RepoWriter;

    this.objectMapper.setPropertyNamingStrategy(
        PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
  }

  @Override
  protected void write() throws Exception {

//    this.influxV9RepoWriter.write(getInfluxPointArry());

  }

  private InfluxPoint[] getInfluxPointArry() throws Exception {

    List<InfluxPoint> influxPointList = new LinkedList<>();

    for (AlarmStateTransitionedEvent event : this.alarmStateTransitionedEventList) {
      Map<String, Object> valueMap = new HashMap<>();

      valueMap.put("tenant_id", event.tenantId);
      valueMap.put("alarm_id", event.alarmId);
      valueMap.put("metrics", this.objectMapper.writeValueAsString(event.metrics));
      valueMap.put("old_state", event.oldState);
      valueMap.put("new_state", event.newState);
      valueMap.put("reason", event.stateChangeReason);
      valueMap.put("reason_data", "{}");

      DateTime dateTime = new DateTime(event.timestamp * 1000, DateTimeZone.UTC);
      String dateString = this.dateFormatter.print(dateTime);

      InfluxPoint
          influxPoint =
          new InfluxPoint(ALARM_STATE_HISTORY_NAME, new HashMap(), dateString, valueMap);

      influxPointList.add(influxPoint);
    }

    return influxPointList.toArray(new InfluxPoint[influxPointList.size()]);
  }
}
