
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.inject.Inject;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.dropwizard.setup.Environment;
import monasca.persister.repository.influxdb.InfluxPoint;

public class InfluxV9MetricRepo extends InfluxMetricRepo {

  private static final Logger logger = LoggerFactory.getLogger(InfluxV9MetricRepo.class);

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final SimpleDateFormat simpleDateFormat =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS zzz");

  private final InfluxV9RepoWriter influxV9RepoWriter;

  @Inject
  public InfluxV9MetricRepo(final Environment env,
                            final InfluxV9RepoWriter influxV9RepoWriter) {

    super(env);
    this.influxV9RepoWriter = influxV9RepoWriter;

  }

  @Override
  protected void write() throws Exception {

    this.influxV9RepoWriter.write(getInfluxPointArry());

  }

  private InfluxPoint[] getInfluxPointArry() throws Exception {

    DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTime();

    List<InfluxPoint> influxPointList = new LinkedList<>();

    for (final Sha1HashId defDimId : this.measurementMap.keySet()) {

      final DefDim defDim = this.defDimMap.get(defDimId);
      final Def def = getDef(defDim.defId);
      final Set<Dim> dimSet = getDimSet(defDim.dimId);

      Map<String, String> tagMap = new HashMap<>();
      for (Dim dim : dimSet) {
        tagMap.put(dim.name, dim.value);
      }
      tagMap.put("tenant_id", def.tenantId);
      tagMap.put("region", def.region);

      for (final Measurement measurement : this.measurementMap.get(defDimId)) {

        Date date = this.simpleDateFormat.parse(measurement.time + " UTC");
        DateTime dateTime = new DateTime(date.getTime(), DateTimeZone.UTC);
        String dateString = dateFormatter.print(dateTime);

        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("value", measurement.value);
        String valueMetaJson = null;
        if (measurement.valueMeta != null && !measurement.valueMeta.isEmpty()) {
          try {
            valueMetaJson = objectMapper.writeValueAsString(measurement.valueMeta);
            logger.debug("Added value for value_meta of {}", valueMetaJson);
          } catch (JsonProcessingException e) {
            logger.error("Unable to serialize " + measurement.valueMeta, e);
            valueMetaJson = null;
          }
        }
        valueMap.put("value_meta", valueMetaJson);
        InfluxPoint influxPoint = new InfluxPoint(def.name, tagMap, dateString, valueMap);

        influxPointList.add(influxPoint);

        this.measurementMeter.mark();
      }
    }

    return influxPointList.toArray(new InfluxPoint[influxPointList.size()]);
  }
}
