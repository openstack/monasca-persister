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

import org.influxdb.dto.Serie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.dropwizard.setup.Environment;

public class InfluxV8MetricRepo extends InfluxMetricRepo
{

  private static final Logger logger = LoggerFactory.getLogger(InfluxV8MetricRepo.class);
  private static final String[] COL_NAMES_STRING_ARRY = {"time", "value"};

  private final InfluxV8RepoWriter influxV8RepoWriter;

  private final SimpleDateFormat simpleDateFormat =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");

  @Inject
  public InfluxV8MetricRepo(final Environment env,
                            final InfluxV8RepoWriter influxV8RepoWriter) {

    super(env);
    this.influxV8RepoWriter = influxV8RepoWriter;

  }

  @Override
  protected void write() throws Exception {

    this.influxV8RepoWriter.write(TimeUnit.SECONDS, getSeries());
  }

  private Serie[] getSeries() throws Exception {

    final List<Serie> serieList = new LinkedList<>();

    for (final Sha1HashId defDimId : this.measurementMap.keySet()) {

      final DefDim defDim = this.defDimMap.get(defDimId);
      final Def def = getDef(defDim.defId);
      final Set<Dim> dimSet = getDimSet(defDim.dimId);
      final Serie.Builder builder = new Serie.Builder(buildSerieName(def, dimSet));

      builder.columns(COL_NAMES_STRING_ARRY);

      for (final Measurement measurement : this.measurementMap.get(defDimId)) {
        final Object[] colValsObjArry = new Object[COL_NAMES_STRING_ARRY.length];
        final Date date = this.simpleDateFormat.parse(measurement.time + " UTC");
        final Long time = date.getTime() / 1000;
        colValsObjArry[0] = time;
        logger.debug("Added column value to colValsObjArry[{}] = {}", 0, colValsObjArry[0]);
        colValsObjArry[1] = measurement.value;
        logger.debug("Added column value to colValsObjArry[{}] = {}", 1, colValsObjArry[1]);
        builder.values(colValsObjArry);
        this.measurementMeter.mark();
      }

      final Serie serie = builder.build();

      if (logger.isDebugEnabled()) {
        this.influxV8RepoWriter.logColValues(serie);
      }

      serieList.add(serie);
      logger.debug("Added serie: {} to serieList", serie.getName());
    }

    return serieList.toArray(new Serie[serieList.size()]);
  }

  private String buildSerieName(final Def def, final Set<Dim> dimList)
      throws UnsupportedEncodingException {

    logger.debug("Creating serie name");

    final StringBuilder serieNameBuilder = new StringBuilder();

    logger.debug("Adding tenant_id to serie name: {}", def.tenantId);
    serieNameBuilder.append(urlEncodeUTF8(def.tenantId));
    serieNameBuilder.append("?");

    logger.debug("Adding region to serie name: {}", def.region);
    serieNameBuilder.append(urlEncodeUTF8(def.region));
    serieNameBuilder.append("&");
    logger.debug("Adding name to serie name: {}", def.name);
    serieNameBuilder.append(urlEncodeUTF8(def.name));

    for (final Dim dim : dimList) {
      serieNameBuilder.append("&");
      logger.debug("Adding dimension name to serie name: {}", dim.name);
      serieNameBuilder.append(urlEncodeUTF8(dim.name));
      serieNameBuilder.append("=");
      logger.debug("Adding dimension value to serie name: {}", dim.value);
      serieNameBuilder.append(urlEncodeUTF8(dim.value));
    }

    final String serieName = serieNameBuilder.toString();
    logger.debug("Created serie name: {}", serieName);

    return serieName;
  }

  private String urlEncodeUTF8(final String s) throws UnsupportedEncodingException {
    return URLEncoder.encode(s, "UTF-8");
  }
}
