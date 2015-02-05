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

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.dropwizard.setup.Environment;
import monasca.persister.configuration.PersisterConfig;

public class InfluxV8RepoWriter {

  static final Logger logger = LoggerFactory.getLogger(InfluxV8RepoWriter.class);

  private final PersisterConfig config;
  private final Environment env;
  private final InfluxDB influxDB;

  private final String databaseName;

  @Inject
  public InfluxV8RepoWriter(final PersisterConfig config, final Environment env) {

    this.config = config;
    this.env = env;
    this.influxDB =
        InfluxDBFactory.connect(config.getInfluxDBConfiguration().getUrl(),
                                config.getInfluxDBConfiguration().getUser(),
                                config.getInfluxDBConfiguration().getPassword());

    this.databaseName = this.config.getInfluxDBConfiguration().getName();

  }

  protected void write(final TimeUnit precision, final Serie[] series) {

    this.influxDB.write(this.databaseName, precision, series);
  }

  protected void logColValues(final Serie serie) {
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

  protected void logColumnNames(final String[] colNames) {
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
