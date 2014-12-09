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

public final class InfluxDBMetricRepository extends InfluxRepository implements MetricRepository {

  private static final Logger logger = LoggerFactory.getLogger(InfluxDBMetricRepository.class);
  private static final int NUMBER_COLUMNS = 2;

  private final Map<Sha1HashId, Def> defMap = new HashMap<>();
  private final Map<Sha1HashId, Set<Dim>> dimMap = new HashMap<>();
  private final Map<Sha1HashId, DefDim> defDimMap = new HashMap<>();
  private final Map<Sha1HashId, List<Measurement>> measurementMap = new HashMap<>();

  private final com.codahale.metrics.Timer flushTimer;
  public final Meter measurementMeter;

  private final SimpleDateFormat measurementTimeSimpleDateFormat = new
      SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");
  private static final Sha1HashId BLANK_SHA_1_HASH_ID = new Sha1HashId(DigestUtils.sha(""));
  private static final Set<Dim> EMPTY_DIM_TREE_SET = new TreeSet<>();

  @Inject
  public InfluxDBMetricRepository(final MonPersisterConfiguration configuration,
                                  final Environment environment) {
    super(configuration, environment);
    this.flushTimer = this.environment.metrics().timer(this.getClass().getName() + "." +
                                                       "flush-timer");
    this.measurementMeter = this.environment.metrics().meter(this.getClass().getName() + "." +
                                                             "measurement-meter");
  }

  @Override
  public void addMetricToBatch(final Sha1HashId defDimsId, final String timeStamp,
                               final double value) {
    final Measurement measurement = new Measurement(defDimsId, timeStamp, value);
    List<Measurement> measurementList = this.measurementMap.get(defDimsId);
    if (measurementList == null) {
      measurementList = new LinkedList<>();
      this.measurementMap.put(defDimsId, measurementList);
    }
    measurementList.add(measurement);
  }

  @Override
  public void addDefinitionToBatch(final Sha1HashId defId, final String name, final String tenantId,
                                   final String region) {
    final Def def = new Def(defId, name, tenantId, region);
    defMap.put(defId, def);
  }

  @Override
  public void addDimensionToBatch(final Sha1HashId dimSetId, final String name,
                                  final String value) {
    Set<Dim> dimSet = dimMap.get(dimSetId);
    if (dimSet == null) {
      dimSet = new TreeSet<>();
      dimMap.put(dimSetId, dimSet);
    }

    final Dim dim = new Dim(dimSetId, name, value);
    dimSet.add(dim);
  }

  @Override
  public void addDefinitionDimensionToBatch(final Sha1HashId defDimsId, final Sha1HashId defId,
                                            Sha1HashId dimId) {
    final DefDim defDim = new DefDim(defDimsId, defId, dimId);
    defDimMap.put(defDimsId, defDim);
  }

  @Override
  public void flush() {

    try {
      final long startTime = System.currentTimeMillis();
      final Timer.Context context = flushTimer.time();
      this.influxDB.write(this.configuration.getInfluxDBConfiguration().getName(),
                          TimeUnit.SECONDS, getSeries());
      final long endTime = System.currentTimeMillis();
      context.stop();
      logger.debug("Writing measurements, definitions, and dimensions to InfluxDB took {} seconds",
                   (endTime - startTime) / 1000);
    } catch (Exception e) {
      logger.error("Failed to write measurements to InfluxDB", e);
    }
    clearBuffers();
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

  private Def getDef(final Sha1HashId defId) throws Exception {

    final Def def = this.defMap.get(defId);
    if (def == null) {
      throw new Exception("Failed to find definition for defId: " + defId);
    }

    return def;
  }

  private Set<Dim> getDimSet(final Sha1HashId dimId) throws Exception {

    // If there were no dimensions, then "" was used in the hash id and nothing was
    // ever added to the dimension map for this dimension set.
    if (dimId.equals(BLANK_SHA_1_HASH_ID)) {
      return EMPTY_DIM_TREE_SET;
    }

    final Set<Dim> dimSet = this.dimMap.get(dimId);

    if (dimSet == null) {
      throw new Exception("Failed to find dimension set for dimId: " + dimId);
    }

    return dimSet;
  }

  private String urlEncodeUTF8(final String s) throws UnsupportedEncodingException {
    return URLEncoder.encode(s, "UTF-8");
  }

  private String[] buildColNamesStringArry() {

    final String[] colNameStringArry = new String[NUMBER_COLUMNS];

    colNameStringArry[0] = "time";
    logger.debug("Added column name[{}] = {}", 0, colNameStringArry[0]);

    colNameStringArry[1] = "value";
    logger.debug("Added column name[{}] = {}", 1, colNameStringArry[1]);

    if (logger.isDebugEnabled()) {
      logColumnNames(colNameStringArry);
    }

    return colNameStringArry;
  }

  private Serie[] getSeries() throws Exception {

    final List<Serie> serieList = new LinkedList<>();

    for (final Sha1HashId defDimId : this.measurementMap.keySet()) {

      final DefDim defDim = this.defDimMap.get(defDimId);
      final Def def = getDef(defDim.defId);
      final Set<Dim> dimSet = getDimSet(defDim.dimId);
      final Builder builder = new Serie.Builder(buildSerieName(def, dimSet));

      builder.columns(buildColNamesStringArry());

      for (final Measurement measurement : this.measurementMap.get(defDimId)) {
        final Object[] colValsObjArry = new Object[NUMBER_COLUMNS];
        final Date date = measurementTimeSimpleDateFormat.parse(measurement.time + " UTC");
        final Long time = date.getTime() / 1000;
        colValsObjArry[0] = time;
        logger.debug("Added column value to colValsObjArry[{}] = {}", 0, colValsObjArry[0]);
        colValsObjArry[1] = measurement.value;
        logger.debug("Added column value to colValsObjArry[{}] = {}", 1, colValsObjArry[1]);
        builder.values(colValsObjArry);
        measurementMeter.mark();
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
    this.defMap.clear();
    this.dimMap.clear();
    this.defDimMap.clear();
  }

  private static final class Measurement {

    final Sha1HashId defDimsId;
    final String time;
    final double value;

    private Measurement(final Sha1HashId defDimsId, final String time, final double value) {
      this.defDimsId = defDimsId;
      this.time = time;
      this.value = value;
    }

    @Override
    public String toString() {
      return "Measurement{" + "defDimsId=" + defDimsId + ", time='" + time + '\'' + ", " +
             "value=" + value + '}';
    }
  }

  private static final class Def {

    final Sha1HashId defId;
    final String name;
    final String tenantId;
    final String region;

    private Def(final Sha1HashId defId, final String name, final String tenantId,
                final String region) {
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

  private static final class Dim implements Comparable<Dim> {

    final Sha1HashId dimSetId;
    final String name;
    final String value;

    private Dim(final Sha1HashId dimSetId, final String name, final String value) {
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
    public int compareTo(Dim o) {
      int nameCmp = String.CASE_INSENSITIVE_ORDER.compare(name, o.name);
      return (nameCmp != 0 ? nameCmp : String.CASE_INSENSITIVE_ORDER.compare(value, o.value));
    }
  }

  private static final class DefDim {

    final Sha1HashId defDimId;
    final Sha1HashId defId;
    final Sha1HashId dimId;

    private DefDim(final Sha1HashId defDimId, final Sha1HashId defId, final Sha1HashId dimId) {
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
