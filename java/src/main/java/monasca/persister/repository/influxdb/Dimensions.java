/*
 * Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.Nullable;

public class Dimensions {

  private static final int MAX_DIMENSIONS_NAME_LENGTH = 255;
  private static final int MAX_DIMENSIONS_VALUE_LENGTH = 255;

  private final Map<String, String> dimensionsMap;


  public Dimensions(@Nullable Map<String, String> dimensionsMap) {

    this.dimensionsMap = new TreeMap<>();

    if (dimensionsMap != null) {

      for (String name : dimensionsMap.keySet()) {

        if (name != null && !name.isEmpty()) {

          String value = dimensionsMap.get(name);

          if (value != null && !value.isEmpty()) {

            if (name.length() > MAX_DIMENSIONS_NAME_LENGTH) {

              name = name.substring(0, MAX_DIMENSIONS_NAME_LENGTH);

            }

            if (value.length() > MAX_DIMENSIONS_VALUE_LENGTH) {

              value = value.substring(0, MAX_DIMENSIONS_VALUE_LENGTH);
            }

            this.dimensionsMap.put(name, value);

          }
        }
      }
    }
  }

  @Override
  public boolean equals(Object o) {

    if (this == o) {
      return true;
    }

    if (!(o instanceof Dimensions)) {
      return false;
    }

    Dimensions that = (Dimensions) o;

    return dimensionsMap.equals(that.dimensionsMap);

  }

  @Override
  public int hashCode() {
    return dimensionsMap.hashCode();
  }

  public Set<String> keySet() {

    return this.dimensionsMap.keySet();

  }

  public Set<Map.Entry<String, String>> entrySet() {

    return this.dimensionsMap.entrySet();

  }

  public String get(String key) {

    return this.dimensionsMap.get(key);

  }
}
