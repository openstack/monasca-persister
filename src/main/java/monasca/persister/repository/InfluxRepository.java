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

import monasca.persister.configuration.MonPersisterConfiguration;

import io.dropwizard.setup.Environment;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class InfluxRepository {
  static final Logger logger = LoggerFactory.getLogger(InfluxRepository.class);

  protected final MonPersisterConfiguration configuration;
  protected final Environment environment;
  protected final InfluxDB influxDB;

  public InfluxRepository(MonPersisterConfiguration configuration, Environment environment) {
    this.configuration = configuration;
    this.environment = environment;
    influxDB =
        InfluxDBFactory.connect(configuration.getInfluxDBConfiguration().getUrl(), configuration
            .getInfluxDBConfiguration().getUser(), configuration.getInfluxDBConfiguration()
            .getPassword());
  }

  protected void logColValues(Serie serie) {
    logger.debug("Added array of array of column values to serie");
    final String[] colNames = serie.getColumns();
    List<Map<String, Object>> rows = serie.getRows();
    int outerIdx = 0;
    for (Map<String, Object> row : rows) {
      StringBuffer sb = new StringBuffer();
      boolean first = true;
      for (String colName : colNames) {
        if (first) {
          first = false;
        } else {
          sb.append(",");
        }
        sb.append(row.get(colName));
      }
      logger.debug("Array of column values[{}]: [{}]", outerIdx, sb);
      outerIdx++;
    }
  }

  protected void logColumnNames(String[] colNames) {
    logger.debug("Added array of column names to serie");
    StringBuffer sb = new StringBuffer();
    boolean first = true;
    for (String colName : colNames) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(colName);
    }
    logger.debug("Array of column names: [{}]", sb);
  }

}
