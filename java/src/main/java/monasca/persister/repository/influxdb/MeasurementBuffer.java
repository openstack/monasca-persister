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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MeasurementBuffer {

  private final Map<Definition, Map<Dimensions, List<Measurement>>>
      measurementMap = new HashMap<>();

  public void put(Definition definition, Dimensions dimensions, Measurement measurement) {

    Map<Dimensions, List<Measurement>> dimensionsMap = this.measurementMap.get(definition);

    if (dimensionsMap == null) {

      dimensionsMap = initDimensionsMap(definition, dimensions);

    }

    List<Measurement> measurementList = dimensionsMap.get(dimensions);

    if (measurementList == null) {

      measurementList = initMeasurementList(dimensionsMap, dimensions);

    }

    measurementList.add(measurement);

  }

  public Set<Map.Entry<Definition, Map<Dimensions, List<Measurement>>>> entrySet() {

    return this.measurementMap.entrySet();

  }

  public void clear() {

    this.measurementMap.clear();

  }

  public boolean isEmpty() {

    return this.measurementMap.isEmpty();

  }

  private Map<Dimensions, List<Measurement>> initDimensionsMap(Definition definition,
                                                               Dimensions dimensions) {

    Map<Dimensions, List<Measurement>> dimensionsMap = new HashMap<>();

    List<Measurement> measurementList = new LinkedList<>();

    dimensionsMap.put(dimensions, measurementList);

    this.measurementMap.put(definition, dimensionsMap);

    return dimensionsMap;
  }

  private List<Measurement> initMeasurementList(Map<Dimensions, List<Measurement>> dimensionsMap,
                                                Dimensions dimensions) {

    List<Measurement> measurementList = new LinkedList<>();

    dimensionsMap.put(dimensions, measurementList);

    return measurementList;

  }

}
