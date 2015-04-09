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

package monasca.persister;

import monasca.persister.consumer.metric.KafkaMetricsConsumer;
import monasca.persister.consumer.metric.MetricsConsumer;
import monasca.persister.pipeline.MetricPipeline;
import monasca.persister.pipeline.event.MetricHandler;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class MonPersisterConsumerTest {

  @Mock
  private KafkaMetricsConsumer kafkaConsumer;

  @Mock
  private MetricsConsumer monConsumer;

  private MetricHandler metricHandler;

  private MetricPipeline metricPipeline;

  @Before
  public void initMocks() {
    metricHandler = Mockito.mock(MetricHandler.class);
    metricPipeline = Mockito.spy(new MetricPipeline(metricHandler));
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testKafkaConsumerLifecycle() throws Exception {
    monConsumer.start();
    monConsumer.stop();
    metricPipeline.shutdown();
    Mockito.verify(metricHandler).flush();
  }
}
