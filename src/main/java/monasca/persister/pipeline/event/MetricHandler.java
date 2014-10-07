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

package monasca.persister.pipeline.event;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import com.codahale.metrics.Counter;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.common.model.metric.MetricEnvelope;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import io.dropwizard.setup.Environment;
import monasca.persister.configuration.PipelineConfiguration;
import monasca.persister.repository.MetricRepository;
import monasca.persister.repository.Sha1HashId;

import static monasca.persister.repository.VerticaMetricsConstants.MAX_COLUMN_LENGTH;

public class MetricHandler extends FlushableHandler<MetricEnvelope[]> {

  private static final Logger logger = LoggerFactory.getLogger(MetricHandler.class);
  private static final String TENANT_ID = "tenantId";
  private static final String REGION = "region";

  private final int ordinal;

  private final SimpleDateFormat simpleDateFormat;

  private final MetricRepository verticaMetricRepository;

  private final Counter metricCounter;
  private final Counter definitionCounter;
  private final Counter dimensionCounter;
  private final Counter definitionDimensionsCounter;

  @Inject
  public MetricHandler(MetricRepository metricRepository,
                       @Assisted PipelineConfiguration configuration, Environment environment,
                       @Assisted("ordinal") int ordinal, @Assisted("batchSize") int batchSize) {
    super(configuration, environment, ordinal, batchSize, MetricHandler.class.getName());
    final String handlerName = String.format("%s[%d]", MetricHandler.class.getName(), ordinal);
    this.verticaMetricRepository = metricRepository;
    this.metricCounter =
        environment.metrics().counter(handlerName + "." + "metrics-added-to-batch-counter");
    this.definitionCounter =
        environment.metrics()
            .counter(handlerName + "." + "metric-definitions-added-to-batch-counter");
    this.dimensionCounter =
        environment.metrics()
            .counter(handlerName + "." + "metric-dimensions-added-to-batch-counter");
    this.definitionDimensionsCounter =
        environment.metrics()
            .counter(handlerName + "." + "metric-definition-dimensions-added-to-batch-counter");

    this.ordinal = ordinal;

    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-0"));
  }

  @Override
  public int process(MetricEnvelope[] metricEnvelopes) throws Exception {
    int metricCount = 0;
    for (final MetricEnvelope metricEnvelope : metricEnvelopes) {
      metricCount += processEnvelope(metricEnvelope);
    }
    return metricCount;
  }

  private int processEnvelope(MetricEnvelope metricEnvelope) {
    int metricCount = 0;
    Metric metric = metricEnvelope.metric;
    Map<String, Object> meta = metricEnvelope.meta;

    logger.debug("ordinal: {}", ordinal);
    logger.debug("metric: {}", metric);
    logger.debug("meta: {}", meta);

    String tenantId = "";
    if (meta.containsKey(TENANT_ID)) {
      tenantId = (String) meta.get(TENANT_ID);
    } else {
      logger.warn(
          "Failed to find tenantId in message envelope meta data. Metric message may be malformed. Setting tenantId to empty string.");
      logger.warn("metric: {}", metric.toString());
      logger.warn("meta: {}", meta.toString());
    }

    String region = "";
    if (meta.containsKey(REGION)) {
      region = (String) meta.get(REGION);
    } else {
      logger.warn(
          "Failed to find region in message envelope meta data. Metric message may be malformed. Setting region to empty string.");
      logger.warn("metric: {}", metric.toString());
      logger.warn("meta: {}", meta.toString());
    }

    // Add the definition to the batch.
    StringBuilder
        definitionIdStringToHash =
        new StringBuilder(trunc(metric.getName(), MAX_COLUMN_LENGTH));
    definitionIdStringToHash.append(trunc(tenantId, MAX_COLUMN_LENGTH));
    definitionIdStringToHash.append(trunc(region, MAX_COLUMN_LENGTH));
    byte[] definitionIdSha1Hash = DigestUtils.sha(definitionIdStringToHash.toString());
    Sha1HashId definitionSha1HashId = new Sha1HashId((definitionIdSha1Hash));
    verticaMetricRepository
        .addDefinitionToBatch(definitionSha1HashId, trunc(metric.getName(), MAX_COLUMN_LENGTH),
                              trunc(tenantId, MAX_COLUMN_LENGTH), trunc(region, MAX_COLUMN_LENGTH));
    definitionCounter.inc();

    // Calculate dimensions sha1 hash id.
    StringBuilder dimensionIdStringToHash = new StringBuilder();
    Map<String, String> preppedDimMap = prepDimensions(metric.getDimensions());
    for (Map.Entry<String, String> entry : preppedDimMap.entrySet()) {
      dimensionIdStringToHash.append(entry.getKey());
      dimensionIdStringToHash.append(entry.getValue());
    }
    byte[] dimensionIdSha1Hash = DigestUtils.sha(dimensionIdStringToHash.toString());
    Sha1HashId dimensionsSha1HashId = new Sha1HashId(dimensionIdSha1Hash);

    // Add the dimension name/values to the batch.
    for (Map.Entry<String, String> entry : preppedDimMap.entrySet()) {
      verticaMetricRepository
          .addDimensionToBatch(dimensionsSha1HashId, entry.getKey(), entry.getValue());
      dimensionCounter.inc();
    }

    // Add the definition dimensions to the batch.
    StringBuilder
        definitionDimensionsIdStringToHash =
        new StringBuilder(definitionSha1HashId.toHexString());
    definitionDimensionsIdStringToHash.append(dimensionsSha1HashId.toHexString());
    byte[]
        definitionDimensionsIdSha1Hash =
        DigestUtils.sha(definitionDimensionsIdStringToHash.toString());
    Sha1HashId definitionDimensionsSha1HashId = new Sha1HashId(definitionDimensionsIdSha1Hash);
    verticaMetricRepository
        .addDefinitionDimensionToBatch(definitionDimensionsSha1HashId, definitionSha1HashId,
                                       dimensionsSha1HashId);
    definitionDimensionsCounter.inc();

    // Add the measurements to the batch.
    if (metric.getTimeValues() != null)

    {
      for (double[] timeValuePairs : metric.getTimeValues()) {
        String timeStamp = simpleDateFormat.format(new Date((long) (timeValuePairs[0] * 1000)));
        double value = timeValuePairs[1];
        verticaMetricRepository.addMetricToBatch(definitionDimensionsSha1HashId, timeStamp, value);
        metricCounter.inc();
        metricCount++;
      }
    } else

    {
      String timeStamp = simpleDateFormat.format(new Date(metric.getTimestamp() * 1000));
      double value = metric.getValue();
      verticaMetricRepository.addMetricToBatch(definitionDimensionsSha1HashId, timeStamp, value);
      metricCounter.inc();
      metricCount++;
    }

    return metricCount;
  }

  @Override
  public void flushRepository() {
    verticaMetricRepository.flush();
  }


  private Map<String, String> prepDimensions(Map<String, String> dimMap) {

    Map<String, String> newDimMap = new TreeMap<>();

    if (dimMap != null) {
      for (String dimName : dimMap.keySet()) {
        if (dimName != null && !dimName.isEmpty()) {
          String dimValue = dimMap.get(dimName);
          if (dimValue != null && !dimValue.isEmpty()) {
            newDimMap.put(trunc(dimName, MAX_COLUMN_LENGTH), trunc(dimValue, MAX_COLUMN_LENGTH));
          }
        }
      }
    }
    return newDimMap;
  }

  private String trunc(String s, int l) {

    if (s == null) {
      return "";
    } else if (s.length() <= l) {
      return s;
    } else {
      String r = s.substring(0, l);
      logger.warn("Input string exceeded max column length. Truncating input string {} to {} chars",
                  s, l);
      logger.warn("Resulting string {}", r);
      return r;
    }
  }
}
