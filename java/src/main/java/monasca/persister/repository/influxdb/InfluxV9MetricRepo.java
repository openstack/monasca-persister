
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

import com.google.inject.Inject;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.dropwizard.setup.Environment;
import monasca.persister.repository.RepoException;

public class InfluxV9MetricRepo extends InfluxMetricRepo {

  private final InfluxV9RepoWriter influxV9RepoWriter;

  @Inject
  public InfluxV9MetricRepo(
      final Environment env,
      final InfluxV9RepoWriter influxV9RepoWriter) {

    super(env);

    this.influxV9RepoWriter = influxV9RepoWriter;

  }

  @Override
  protected int write(String id) throws RepoException {

    return this.influxV9RepoWriter.write(getInfluxPointArry(), id);

  }

  private InfluxPoint[] getInfluxPointArry() {

    List<InfluxPoint> influxPointList = new LinkedList<>();

    for (Map.Entry<Definition, Map<Dimensions, List<Measurement>>> definitionMapEntry
        : this.measurementBuffer.entrySet()) {

      Definition definition = definitionMapEntry.getKey();
      Map<Dimensions, List<Measurement>> dimensionsMap = definitionMapEntry.getValue();

      for (Map.Entry<Dimensions, List<Measurement>> dimensionsMapEntry
          : dimensionsMap.entrySet()) {

        Dimensions dimensions = dimensionsMapEntry.getKey();
        List<Measurement> measurementList = dimensionsMapEntry.getValue();

        Map<String, String> tagMap = buildTagMap(definition, dimensions);

        for (Measurement measurement : measurementList) {

          InfluxPoint
              influxPoint =
              new InfluxPoint(definition.getName(),
                              tagMap,
                              measurement.getISOFormattedTimeString(),
                              buildValueMap(measurement));

          influxPointList.add(influxPoint);

        }
      }
    }

    return influxPointList.toArray(new InfluxPoint[influxPointList.size()]);

  }

  private Map<String, Object> buildValueMap(Measurement measurement) {

    Map<String, Object> valueMap = new HashMap<>();

    valueMap.put("value", measurement.getValue());

    String valueMetaJSONString = measurement.getValueMetaJSONString();

    if (valueMetaJSONString == null || valueMetaJSONString.isEmpty()) {

      valueMap.put("value_meta", "{}");

    } else {

      valueMap.put("value_meta", valueMetaJSONString);

    }

    return valueMap;

  }

  private Map<String, String> buildTagMap(Definition definition, Dimensions dimensions) {

    Map<String,String> tagMap = new HashMap<>();

    for (Map.Entry<String, String> dimensionsEntry : dimensions.entrySet()) {

      String name = dimensionsEntry.getKey();

      String value = dimensionsEntry.getValue();

      tagMap.put(name, value);

    }

    tagMap.put("_tenant_id", definition.getTenantId());

    tagMap.put("_region", definition.getRegion());

    return tagMap;

  }
}
