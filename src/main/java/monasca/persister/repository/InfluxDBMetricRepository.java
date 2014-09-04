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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import org.apache.commons.codec.digest.DigestUtils;
import org.influxdb.dto.Serie;
import org.influxdb.dto.Serie.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import io.dropwizard.setup.Environment;
import monasca.persister.configuration.MonPersisterConfiguration;

public class InfluxDBMetricRepository extends InfluxRepository implements MetricRepository {

  private static final Logger logger = LoggerFactory.getLogger(InfluxDBMetricRepository.class);

  private final Map<Sha1HashId, Definition> definitionMap = new HashMap<>();
  private final Map<Sha1HashId, Set<Dimension>> dimensionMap = new HashMap<>();
  private final Map<Sha1HashId, DefinitionDimension> definitionDimensionMap = new HashMap<>();
  private final Map<Sha1HashId, List<Measurement>> measurementMap = new HashMap<>();

  private final com.codahale.metrics.Timer flushTimer;
  public final Meter measurementMeter;

  private final SimpleDateFormat measurementTimeStampSimpleDateFormat = new
      SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");
  private static final Sha1HashId BLANK_SHA_1_HASH_ID = new Sha1HashId(DigestUtils.sha(""));
  private static final Set<Dimension> EMPTY_DIMENSION_TREE_SET = new TreeSet();

  @Inject
  public InfluxDBMetricRepository(MonPersisterConfiguration configuration,
                                  Environment environment) {
    super(configuration, environment);
    this.flushTimer = this.environment.metrics().timer(this.getClass().getName() + "." +
                                                       "flush-timer");
    this.measurementMeter = this.environment.metrics().meter(this.getClass().getName() + "." +
                                                             "measurement-meter");
  }

  @Override
  public void addMetricToBatch(Sha1HashId defDimsId, String timeStamp, double value) {
    Measurement m = new Measurement(defDimsId, timeStamp, value);
    List<Measurement> measurementList = this.measurementMap.get(defDimsId);
    if (measurementList == null) {
      measurementList = new LinkedList<>();
      this.measurementMap.put(defDimsId, measurementList);
    }
    measurementList.add(m);
  }

  @Override
  public void addDefinitionToBatch(Sha1HashId defId, String name, String tenantId, String region) {
    Definition d = new Definition(defId, name, tenantId, region);
    definitionMap.put(defId, d);
  }

  @Override
  public void addDimensionToBatch(Sha1HashId dimSetId, String name, String value) {
    Set<Dimension> dimensionSet = dimensionMap.get(dimSetId);
    if (dimensionSet == null) {
      dimensionSet = new TreeSet<Dimension>();
      dimensionMap.put(dimSetId, dimensionSet);
    }

    Dimension d = new Dimension(dimSetId, name, value);
    dimensionSet.add(d);
  }

  @Override
  public void addDefinitionDimensionToBatch(Sha1HashId defDimsId, Sha1HashId defId,
                                            Sha1HashId dimId) {
    DefinitionDimension dd = new DefinitionDimension(defDimsId, defId, dimId);
    definitionDimensionMap.put(defDimsId, dd);
  }

  @Override
  public void flush() {

    try {
      long startTime = System.currentTimeMillis();
      Timer.Context context = flushTimer.time();
      Serie[] series = getSeries();
      this.influxDB.write(this.configuration.getInfluxDBConfiguration().getName(),
                          TimeUnit.SECONDS, series);
      long endTime = System.currentTimeMillis();
      context.stop();
      logger.debug("Writing measurements, definitions, and dimensions to database took {} seconds",
                   (endTime - startTime) / 1000);
    } catch (Exception e) {
      logger.error("Failed to write measurements to database", e);
    }
    clearBuffers();
  }

  private String buildSerieName(Definition definition, Set<Dimension> dimensionList)
      throws UnsupportedEncodingException {

    logger.debug("Creating serie name");

    StringBuilder serieNameBuilder = new StringBuilder();

    logger.debug("Adding name to serie name: {}", definition.name);
    serieNameBuilder.append(urlEncodeUTF8(definition.name));
    serieNameBuilder.append("?");

    logger.debug("Adding tenant_id to serie name: {}", definition.tenantId);
    serieNameBuilder.append(urlEncodeUTF8(definition.tenantId));
    serieNameBuilder.append("&");

    logger.debug("Adding region to serie name: {}", definition.region);
    serieNameBuilder.append(urlEncodeUTF8(definition.region));

    for (Dimension dimension : dimensionList) {
      serieNameBuilder.append("&");
      logger.debug("Adding dimension name to serie name: {}", dimension.name);
      serieNameBuilder.append(urlEncodeUTF8(dimension.name));

      serieNameBuilder.append("=");

      logger.debug("Adding dimension value to serie name: {}", dimension.value);
      serieNameBuilder.append(urlEncodeUTF8(dimension.value));
    }

    String serieName = serieNameBuilder.toString();
    logger.debug("Created serie name: {}", serieName);

    return serieName;
  }

  private String urlEncodeUTF8(String s) throws UnsupportedEncodingException {

    return URLEncoder.encode(s, "UTF-8");
  }

  private Serie[] getSeries() throws
                              Exception {

    List<Serie> serieList = new LinkedList<>();

    for (Sha1HashId defdimsId : this.measurementMap.keySet()) {

      DefinitionDimension definitionDimension = this.definitionDimensionMap.get(defdimsId);

      Definition definition = definitionMap.get(definitionDimension.defId);
      if (definition == null) {
        throw new Exception("Failed to find Definition for defId: " + definitionDimension.defId);
      }

      Set<Dimension> dimensionSet;
      // If there were no dimensions, then "" was used in the hash id and nothing was
      // added for dimensions.
      if (definitionDimension.dimId.equals(BLANK_SHA_1_HASH_ID)) {
        dimensionSet = EMPTY_DIMENSION_TREE_SET;
      } else {
        dimensionSet = this.dimensionMap.get(definitionDimension.dimId);
      }

      String serieName = buildSerieName(definition, dimensionSet);
      Builder builder = new Serie.Builder(serieName);
      logger.debug("Created serie: {}", serieName);

      String[] colNameStringArry = new String[2];

      colNameStringArry[0] = "time";
      logger.debug("Added column name[{}]: time", 0);

      colNameStringArry[1] = "value";
      logger.debug("Added column name[{}]: value", 1);

      builder.columns(colNameStringArry);

      if (logger.isDebugEnabled()) {
        logColumnNames(colNameStringArry);
      }

      int i = 0;
      for (Measurement measurement : this.measurementMap.get(defdimsId)) {
        Object[] colValsObjArry = new Object[2];
        Date date = measurementTimeStampSimpleDateFormat.parse(measurement.timeStamp + " UTC");
        Long time = date.getTime() / 1000;
        colValsObjArry[0] = time;
        logger.debug("Added column value[{}][{}]: {}", i, 0, time);
        colValsObjArry[1] = measurement.value;
        logger.debug("Added column value[{}][{}]: {}", i, 1, measurement.value);
        builder.values(colValsObjArry);
        measurementMeter.mark();
        i++;
      }

      final Serie serie = builder.build();

      if (logger.isDebugEnabled()) {
        logColValues(serie);
      }

      serieList.add(serie);
      logger.debug("Added serie: {} to serieList", serie.getName());
    }

    return serieList.toArray(new Serie[serieList.size()]);
  }


  private void clearBuffers() {

    this.measurementMap.clear();
    this.definitionMap.clear();
    this.dimensionMap.clear();
    this.definitionDimensionMap.clear();
  }

  private static final class Measurement {

    Sha1HashId defDimsId;
    String timeStamp;
    double value;

    private Measurement(Sha1HashId defDimsId, String timeStamp, double value) {
      this.defDimsId = defDimsId;
      this.timeStamp = timeStamp;
      this.value = value;
    }

    @Override
    public String toString() {
      return "Measurement{" + "defDimsId=" + defDimsId + ", timeStamp='" + timeStamp + '\'' + ", " +
             "value=" + value + '}';
    }
  }

  private static final class Definition {

    Sha1HashId defId;
    String name;
    String tenantId;
    String region;

    private Definition(Sha1HashId defId, String name, String tenantId, String region) {
      this.defId = defId;
      this.name = name;
      this.tenantId = tenantId;
      this.region = region;
    }

    @Override
    public String toString() {
      return "Definition{" + "defId=" + defId + ", name='" + name + '\'' + ", " +
             "tenantId='" + tenantId + '\'' + ", region='" + region + '\'' + '}';
    }
  }

  private static final class Dimension implements Comparable<Dimension> {

    Sha1HashId dimSetId;
    String name;
    String value;

    private Dimension(Sha1HashId dimSetId, String name, String value) {
      this.dimSetId = dimSetId;
      this.name = name;
      this.value = value;
    }

    @Override
    public String toString() {
      return "Dimension{" + "dimSetId=" + dimSetId + ", name='" + name + '\'' + ", " +
             "value='" + value + '\'' + '}';
    }

    @Override
    public int compareTo(Dimension o) {
      int nameCmp = String.CASE_INSENSITIVE_ORDER.compare(name, o.name);
      return (nameCmp != 0 ? nameCmp : String.CASE_INSENSITIVE_ORDER.compare(value, o.value));
    }
  }

  private static final class DefinitionDimension {

    Sha1HashId defDimId;
    Sha1HashId defId;
    Sha1HashId dimId;

    private DefinitionDimension(Sha1HashId defDimId, Sha1HashId defId, Sha1HashId dimId) {
      this.defDimId = defDimId;
      this.defId = defId;
      this.dimId = dimId;
    }

    @Override
    public String toString() {
      return "DefinitionDimension{" + "defDimId=" + defDimId + ", defId=" + defId + ", " +
             "dimId=" + dimId + '}';
    }
  }
}
