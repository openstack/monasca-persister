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

public class InfluxPoint {

  private final String measurement;
  private final Map<String, String> tags;
  private final String time;
  private final Map<String, Object> fields;
  private final String Precision = "ms";

  public InfluxPoint(
      final String measurement,
      final Map<String, String> tags,
      final String time,
      final Map<String, Object> fields) {

    this.measurement = measurement;
    this.tags = tags;
    this.time = time;
    this.fields = fields;
  }

  public String getMeasurement() {
    return measurement;
  }

  public Map<String, String> getTags() {
    return this.tags;
  }

  public String getTime() {
    return this.time;
  }

  public Map<String, Object> getFields() {
    return this.fields;
  }

  public String getPrecision() {
    return Precision;
  }

}
