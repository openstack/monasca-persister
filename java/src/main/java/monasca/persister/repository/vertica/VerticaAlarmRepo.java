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

package monasca.persister.repository.vertica;

import monasca.common.model.event.AlarmStateTransitionedEvent;
import monasca.persister.configuration.PersisterConfig;
import monasca.persister.repository.Repo;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.inject.Inject;

import io.dropwizard.setup.Environment;
import monasca.persister.repository.RepoException;

public class VerticaAlarmRepo extends VerticaRepo implements Repo<AlarmStateTransitionedEvent> {

  private static final Logger logger = LoggerFactory.getLogger(VerticaAlarmRepo.class);
  private final Environment environment;

  private static final String SQL_INSERT_INTO_ALARM_HISTORY =
      "insert into MonAlarms.StateHistory (tenant_id, alarm_id, metrics, old_state, new_state, sub_alarms, reason, reason_data, time_stamp) "
      + "values (:tenant_id, :alarm_id, :metrics, :old_state, :new_state, :sub_alarms, :reason, :reason_data, :time_stamp)";
  private static final int MAX_BYTES_PER_CHAR = 4;
  private static final int MAX_LENGTH_VARCHAR = 65000;

  private PreparedBatch batch;
  private final Timer commitTimer;
  private final SimpleDateFormat simpleDateFormat;

  private int msgCnt = 0;

  private ObjectMapper objectMapper = new ObjectMapper();

  @Inject
  public VerticaAlarmRepo(
      DBI dbi,
      PersisterConfig configuration,
      Environment environment) throws NoSuchAlgorithmException, SQLException {

    super(dbi);

    logger.debug("Instantiating " + this.getClass().getName());

    this.environment = environment;

    this.commitTimer =
        this.environment.metrics().timer(this.getClass().getName() + "." + "commit-timer");

    this.objectMapper.setPropertyNamingStrategy(
        PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    logger.debug("preparing batches...");

    handle.getConnection().setAutoCommit(false);

    batch = handle.prepareBatch(SQL_INSERT_INTO_ALARM_HISTORY);

    handle.begin();

    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-0"));

    logger.debug(this.getClass().getName() + " is fully instantiated");

  }

  public void addToBatch(AlarmStateTransitionedEvent message, String id) {

    String metricsString = getSerializedString(message.metrics, id);

    // Validate metricsString does not exceed a sufficient maximum upper bound
    if (metricsString.length()*MAX_BYTES_PER_CHAR >= MAX_LENGTH_VARCHAR) {
      metricsString = "[]";
      logger.warn("length of metricsString for alarm ID {} exceeds max length of {}", message.alarmId, MAX_LENGTH_VARCHAR);
    }

    String subAlarmsString = getSerializedString(message.subAlarms, id);

    // Validate subAlarmsString does not exceed a sufficient maximum upper bound
    if (subAlarmsString.length()*MAX_BYTES_PER_CHAR >= MAX_LENGTH_VARCHAR) {
      subAlarmsString = "[]";
      logger.warn("length of subAlarmsString for alarm ID {} exceeds max length of {}", message.alarmId, MAX_LENGTH_VARCHAR);
    }

    String timeStamp = simpleDateFormat.format(new Date(message.timestamp));

    batch.add()
        .bind("tenant_id", message.tenantId)
        .bind("alarm_id", message.alarmId)
        .bind("metrics", metricsString)
        .bind("old_state", message.oldState.name())
        .bind("new_state", message.newState.name())
        .bind("sub_alarms", subAlarmsString)
        .bind("reason", message.stateChangeReason)
        .bind("reason_data", "{}")
        .bind("time_stamp", timeStamp);

    this.msgCnt++;
  }

  private String getSerializedString(Object o, String id) {

    try {

      return this.objectMapper.writeValueAsString(o);

    } catch (JsonProcessingException e) {

      logger.error("[[}]: failed to serialize object {}", id, o, e);

      return "";

    }
  }

  public int flush(String id) throws RepoException {

    try {

      commitBatch(id);

      int commitCnt = this.msgCnt;

      this.msgCnt = 0;

      return commitCnt;

    } catch (Exception e) {

      logger.error("[{}]: failed to write alarms to vertica", id, e);

      throw new RepoException("failed to commit batch to vertica", e);

    }
  }

  private void commitBatch(String id) {

    long startTime = System.currentTimeMillis();

    Timer.Context context = commitTimer.time();

    batch.execute();

    handle.commit();

    handle.begin();

    context.stop();

    long endTime = System.currentTimeMillis();

    logger.debug("[{}]: committing batch took {} ms", id, endTime - startTime);

  }
}
