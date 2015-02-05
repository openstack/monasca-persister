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

  private final String name;
  private final Map<String, String> tags;
  private final String timestamp;
  private final Map<String, Object> values;

  public InfluxPoint(final String name, final Map<String, String> tags, final String timestamp,
                     final Map<String, Object> values) {
    this.name = name;
    this.tags = tags;
    this.timestamp = timestamp;
    this.values = values;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getTags() {
    return this.tags;
  }

  public String getTimestamp() {
    return this.timestamp;
  }

  public Map<String, Object> getValues() {
    return this.values;
  }

}
