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

import org.influxdb.dto.Serie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.dropwizard.setup.Environment;

public class InfluxV8MetricRepo extends InfluxMetricRepo
{

  private static final Logger logger = LoggerFactory.getLogger(InfluxV8MetricRepo.class);
  private static final String[] COL_NAMES_STRING_ARRY = {"time", "value", "value_meta"};

  private final InfluxV8RepoWriter influxV8RepoWriter;

  @Inject
  public InfluxV8MetricRepo(final Environment env,
                            final InfluxV8RepoWriter influxV8RepoWriter) {

    super(env);
    this.influxV8RepoWriter = influxV8RepoWriter;

  }

  @Override
  protected void write() throws Exception {

    this.influxV8RepoWriter.write(TimeUnit.MILLISECONDS, getSeries());
  }

  private Serie[] getSeries() throws UnsupportedEncodingException, ParseException {

    final List<Serie> serieList = new LinkedList<>();

    for (Map.Entry<Definition, Map<Dimensions, List<Measurement>>> definitionMapEntry
        : this.measurementBuffer.entrySet()) {

      Definition definition = definitionMapEntry.getKey();
      Map<Dimensions, List<Measurement>> dimensionsMap = definitionMapEntry.getValue();

      for (Map.Entry<Dimensions, List<Measurement>> dimensionsMapEntry : dimensionsMap.entrySet()) {

        Dimensions dimensions = dimensionsMapEntry.getKey();
        List<Measurement> measurementList = dimensionsMapEntry.getValue();

        final Serie.Builder builder = new Serie.Builder(buildSerieName(definition, dimensions));
        builder.columns(COL_NAMES_STRING_ARRY);

        for (Measurement measurement : measurementList) {

          final Object[] valObjArry = new Object[COL_NAMES_STRING_ARRY.length];

          valObjArry[0] = measurement.getTime();
          logger.debug("Added column value to valObjArry[{}] = {}", 0, valObjArry[0]);

          valObjArry[1] = measurement.getValue();
          logger.debug("Added column value to valObjArry[{}] = {}", 1, valObjArry[1]);

          valObjArry[2] = measurement.getValueMetaJSONString();
          logger.debug("Added column value to valObjArry[{}] = {}", 2, valObjArry[2]);

          builder.values(valObjArry);

          this.measurementMeter.mark();

        }

        final Serie serie = builder.build();

        if (logger.isDebugEnabled()) {
          this.influxV8RepoWriter.logColValues(serie);
        }

        serieList.add(serie);
        logger.debug("Added serie: {} to serieList", serie.getName());

      }

    }

    return serieList.toArray(new Serie[serieList.size()]);

  }

  private String buildSerieName(final Definition definition, final Dimensions dimensions)
      throws UnsupportedEncodingException {

    logger.debug("Creating serie name");

    final StringBuilder serieNameBuilder = new StringBuilder();

    logger.debug("Adding tenant_id to serie name: {}", definition.getTenantId());
    serieNameBuilder.append(urlEncodeUTF8(definition.getTenantId()));
    serieNameBuilder.append("?");

    logger.debug("Adding region to serie name: {}", definition.getRegion());
    serieNameBuilder.append(urlEncodeUTF8(definition.getRegion()));
    serieNameBuilder.append("&");
    logger.debug("Adding name to serie name: {}", definition.getName());
    serieNameBuilder.append(urlEncodeUTF8(definition.getName()));

    for (final String name : dimensions.keySet()) {
      final String value = dimensions.get(name);
      serieNameBuilder.append("&");
      logger.debug("Adding dimension name to serie name: {}", name);
      serieNameBuilder.append(urlEncodeUTF8(name));
      serieNameBuilder.append("=");
      logger.debug("Adding dimension value to serie name: {}", value);
      serieNameBuilder.append(urlEncodeUTF8(value));
    }

    final String serieName = serieNameBuilder.toString();
    logger.debug("Created serie name: {}", serieName);

    return serieName;
  }


  private String urlEncodeUTF8(final String s) throws UnsupportedEncodingException {
    return URLEncoder.encode(s, "UTF-8");
  }
}
