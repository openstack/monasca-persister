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

import monasca.common.model.metric.Metric;
import monasca.common.model.metric.MetricEnvelope;

import com.codahale.metrics.Meter;

import java.util.Map;

import io.dropwizard.setup.Environment;

public abstract class InfluxMetricRepo extends InfluxRepo<MetricEnvelope> {

  protected final MeasurementBuffer measurementBuffer = new MeasurementBuffer();

  protected final Meter measurementMeter;

  public InfluxMetricRepo(final Environment env) {

    super(env);

    this.measurementMeter =
        env.metrics().meter(this.getClass().getName() + ".measurement-meter");

  }

  @Override
  public void addToBatch(MetricEnvelope metricEnvelope) {

    Metric metric = metricEnvelope.metric;

    Map<String, Object> meta = metricEnvelope.meta;

    Definition definition =
        new Definition(
            metric.getName(),
            (String) meta.get("tenantId"),
            (String) meta.get("region"));

    Dimensions dimensions = new Dimensions(metric.getDimensions());

    Measurement measurement =
        new Measurement(
            metric.getTimestamp(),
            metric.getValue(),
            metric.getValueMeta());

    this.measurementBuffer.put(definition, dimensions, measurement);

    this.measurementMeter.mark();

  }

  @Override
  protected void clearBuffers() {

    this.measurementBuffer.clear();

  }

  @Override
  protected boolean isBufferEmpty() {

    return this.measurementBuffer.isEmpty();

  }
}
