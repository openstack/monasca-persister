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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import io.dropwizard.setup.Environment;

public abstract class InfluxMetricRepo implements MetricRepo {

  private static final Logger logger = LoggerFactory.getLogger(InfluxMetricRepo.class);

  protected final Map<Sha1HashId, Def> defMap = new HashMap<>();
  protected final Map<Sha1HashId, Set<Dim>> dimMap = new HashMap<>();
  protected final Map<Sha1HashId, DefDim> defDimMap = new HashMap<>();
  protected final Map<Sha1HashId, List<Measurement>> measurementMap = new HashMap<>();

  public final com.codahale.metrics.Timer flushTimer;
  public final Meter measurementMeter;

  private static final Sha1HashId BLANK_SHA_1_HASH_ID = new Sha1HashId(DigestUtils.sha(""));
  private static final Set<Dim> EMPTY_DIM_TREE_SET = new TreeSet<>();

  protected abstract void write() throws Exception;

  public InfluxMetricRepo(final Environment env) {

    this.flushTimer = env.metrics().timer(this.getClass().getName() + "." +
                                          "flush-timer");

    this.measurementMeter = env.metrics().meter(this.getClass().getName() + "." +
                                                "measurement-meter");
  }

  @Override
  public void addMetricToBatch(final Sha1HashId defDimsId, final String timeStamp,
                               final double value, final Map<String, String> valueMeta) {
    final Measurement measurement = new Measurement(defDimsId, timeStamp, value, valueMeta);
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
    this.defMap.put(defId, def);
  }

  @Override
  public void addDimensionToBatch(final Sha1HashId dimSetId, final String name,
                                  final String value) {
    final Dim dim = new Dim(dimSetId, name, value);
    Set<Dim> dimSet = this.dimMap.get(dimSetId);
    if (dimSet == null) {
      dimSet = new TreeSet<>();
      this.dimMap.put(dimSetId, dimSet);
    }

    dimSet.add(dim);
  }

  @Override
  public void addDefinitionDimensionToBatch(final Sha1HashId defDimsId, final Sha1HashId defId,
                                            Sha1HashId dimId) {
    final DefDim defDim = new DefDim(defDimsId, defId, dimId);
    this.defDimMap.put(defDimsId, defDim);
  }

  @Override
  public void flush() {

    try {
      final long startTime = System.currentTimeMillis();
      final Timer.Context context = flushTimer.time();

      write();

      final long endTime = System.currentTimeMillis();
      context.stop();

      logger.debug("Writing measurements, definitions, and dimensions to InfluxDB took {} seconds",
                   (endTime - startTime) / 1000);

    } catch (Exception e) {
      logger.error("Failed to write measurements to InfluxDB", e);
    }

    clearBuffers();
  }

   protected Def getDef(final Sha1HashId defId) throws Exception {

    final Def def = this.defMap.get(defId);
    if (def == null) {
      throw new Exception("Failed to find definition for defId: " + defId);
    }

    return def;
  }

  protected Set<Dim> getDimSet(final Sha1HashId dimId) throws Exception {

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

  private void clearBuffers() {

    this.measurementMap.clear();
    this.defMap.clear();
    this.dimMap.clear();
    this.defDimMap.clear();
  }

   static protected final class Measurement {

    protected final Sha1HashId defDimsId;
    protected final String time;
    protected final double value;
    protected final Map<String, String> valueMeta;

    private Measurement(final Sha1HashId defDimsId, final String time, final double value,
                        final Map<String, String> valueMeta) {
      this.defDimsId = defDimsId;
      this.time = time;
      this.value = value;
      this.valueMeta = valueMeta;
    }

    @Override
    public String toString() {
      return "Measurement{" + "defDimsId=" + this.defDimsId + ", time='" + this.time + '\'' + ", " +
             "value=" + this.value + '}';
    }
  }

   static protected final class Def {

    protected final Sha1HashId defId;
    protected final String name;
    protected final String tenantId;
    protected final String region;

    private Def(final Sha1HashId defId, final String name, final String tenantId,
                final String region) {
      this.defId = defId;
      this.name = name;
      this.tenantId = tenantId;
      this.region = region;
    }

    @Override
    public String toString() {
      return "Definition{" + "defId=" + this.defId + ", name='" + this.name + '\'' + ", " +
             "tenantId='" + this.tenantId + '\'' + ", region='" + this.region + '\'' + '}';
    }
  }

   static protected final class Dim implements Comparable<Dim> {

    protected final Sha1HashId dimSetId;
    protected final String name;
    protected final String value;

    private Dim(final Sha1HashId dimSetId, final String name, final String value) {
      this.dimSetId = dimSetId;
      this.name = name;
      this.value = value;
    }

    @Override
    public String toString() {
      return "Dimension{" + "dimSetId=" + this.dimSetId + ", name='" + this.name + '\'' + ", " +
             "value='" + this.value + '\'' + '}';
    }

    @Override
    public int compareTo(Dim o) {
      int nameCmp = String.CASE_INSENSITIVE_ORDER.compare(name, o.name);
      return (nameCmp != 0 ? nameCmp : String.CASE_INSENSITIVE_ORDER.compare(this.value, o.value));
    }
  }

   static protected final class DefDim {

    protected final Sha1HashId defDimId;
    protected final Sha1HashId defId;
    protected final Sha1HashId dimId;

    private DefDim(final Sha1HashId defDimId, final Sha1HashId defId, final Sha1HashId dimId) {
      this.defDimId = defDimId;
      this.defId = defId;
      this.dimId = dimId;
    }

    @Override
    public String toString() {
      return "DefinitionDimension{" + "defDimId=" + this.defDimId + ", defId=" + this.defId + ", " +
             "dimId=" + this.dimId + '}';
    }
  }
}
