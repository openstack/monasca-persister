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

package monasca.persister.repository.influxdb;

import java.util.Map;

public class InfluxWrite {

  private final String database;
  private final String retentionPolicy;
  private final InfluxPoint[] points;
  private final Map<String, String> tags;


  public InfluxWrite(final String database, final String retentionPolicy, final InfluxPoint[] points,
                     final Map<String, String> tags) {
    this.database = database;
    this.retentionPolicy = retentionPolicy;
    this.points = points;
    this.tags = tags;
  }

  public String getDatabase() {
    return database;
  }

  public String getRetentionPolicy() {
    return retentionPolicy;
  }

  public Map<String, String> getTags() {
    return this.tags;
  }

  public InfluxPoint[] getPoints() {
    return points;
  }
}
