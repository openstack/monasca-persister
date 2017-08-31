/*
 * Copyright (c) 2017 SUSE LLC
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

package monasca.persister.repository.cassandra;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.Timestamp;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import io.dropwizard.setup.Environment;
import monasca.common.model.event.AlarmStateTransitionedEvent;
import monasca.persister.configuration.PersisterConfig;
import monasca.persister.repository.Repo;
import monasca.persister.repository.RepoException;

/**
 * This class is not thread safe.
 *
 */
public class CassandraAlarmRepo extends CassandraRepo implements Repo<AlarmStateTransitionedEvent> {

  private static final Logger logger = LoggerFactory.getLogger(CassandraAlarmRepo.class);

  private static String EMPTY_REASON_DATA = "{}";

  private static final int MAX_BYTES_PER_CHAR = 4;
  private static final int MAX_LENGTH_VARCHAR = 65000;

  private int retention;

  private ObjectMapper objectMapper = new ObjectMapper();

  @Inject
  public CassandraAlarmRepo(CassandraCluster cluster, PersisterConfig config, Environment environment)
      throws NoSuchAlgorithmException, SQLException {
    super(cluster, environment, config.getCassandraDbConfiguration().getMaxWriteRetries(),
        config.getAlarmHistoryConfiguration().getBatchSize());

    this.retention = config.getCassandraDbConfiguration().getRetentionPolicy() * 24 * 3600;

    logger.debug("Instantiating " + this.getClass().getName());

    this.objectMapper
        .setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    session = cluster.getAlarmsSession();

    logger.debug(this.getClass().getName() + " is fully instantiated");

  }

  public void addToBatch(AlarmStateTransitionedEvent message, String id) {

    String metricsString = getSerializedString(message.metrics, id);

    // Validate metricsString does not exceed a sufficient maximum upper bound
    if (metricsString.length() * MAX_BYTES_PER_CHAR >= MAX_LENGTH_VARCHAR) {
      metricsString = "[]";
      logger.warn("length of metricsString for alarm ID {} exceeds max length of {}", message.alarmId,
          MAX_LENGTH_VARCHAR);
    }

    String subAlarmsString = getSerializedString(message.subAlarms, id);

    if (subAlarmsString.length() * MAX_BYTES_PER_CHAR >= MAX_LENGTH_VARCHAR) {
      subAlarmsString = "[]";
      logger.warn("length of subAlarmsString for alarm ID {} exceeds max length of {}", message.alarmId,
          MAX_LENGTH_VARCHAR);
    }

    queue.offerLast(cluster.getAlarmHistoryInsertStmt().bind(retention, metricsString, message.oldState.name(),
        message.newState.name(), subAlarmsString, message.stateChangeReason, EMPTY_REASON_DATA,
        message.tenantId, message.alarmId, new Timestamp(message.timestamp)));
  }

  private String getSerializedString(Object o, String id) {

    try {
      return this.objectMapper.writeValueAsString(o);
    } catch (JsonProcessingException e) {
      logger.error("[[}]: failed to serialize object {}", id, o, e);
      return "";
    }
  }

  @Override
  public int flush(String id) throws RepoException {
    return handleFlush(id);
  }
}
