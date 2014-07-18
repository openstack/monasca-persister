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

package com.hpcloud.mon.persister.consumer;

import com.hpcloud.mon.persister.configuration.KafkaConfiguration;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.configuration.PipelineConfiguration;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaChannel {
  private static final String KAFKA_CONFIGURATION = "Kafka configuration:";
  private static final Logger logger = LoggerFactory.getLogger(KafkaChannel.class);

  private final String topic;
  private final ConsumerConnector consumerConnector;
  private final int threadNum;

  @Inject
  public KafkaChannel(@Assisted MonPersisterConfiguration configuration,
      @Assisted PipelineConfiguration pipelineConfiguration, @Assisted int threadNum) {
    this.topic = pipelineConfiguration.getTopic();
    this.threadNum = threadNum;
    Properties kafkaProperties =
        createKafkaProperties(configuration.getKafkaConfiguration(), pipelineConfiguration);
    consumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig(kafkaProperties));
  }

  public final void markRead() {
    this.consumerConnector.commitOffsets();
  }

  public KafkaStream<byte[], byte[]> getKafkaStream() {
    final Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(this.topic, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> streamMap =
        this.consumerConnector.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = streamMap.values().iterator().next();
    if (streams.size() != 1) {
      throw new IllegalStateException(String.format(
          "Expected only one stream but instead there are %d", streams.size()));
    }
    return streams.get(0);
  }

  public void stop() {
    this.consumerConnector.shutdown();
  }

  private ConsumerConfig createConsumerConfig(Properties kafkaProperties) {
    return new ConsumerConfig(kafkaProperties);
  }

  private Properties createKafkaProperties(KafkaConfiguration kafkaConfiguration,
      final PipelineConfiguration pipelineConfiguration) {
    Properties properties = new Properties();

    properties.put("group.id", pipelineConfiguration.getGroupId());
    properties.put("zookeeper.connect", kafkaConfiguration.getZookeeperConnect());
    properties.put("consumer.id",
        String.format("%s_%d", pipelineConfiguration.getConsumerId(), this.threadNum));
    properties.put("socket.timeout.ms", kafkaConfiguration.getSocketTimeoutMs().toString());
    properties.put("socket.receive.buffer.bytes", kafkaConfiguration.getSocketReceiveBufferBytes()
        .toString());
    properties.put("fetch.message.max.bytes", kafkaConfiguration.getFetchMessageMaxBytes()
        .toString());
    // Set auto commit to false because the persister is going to explicitly commit
    properties.put("auto.commit.enable", "false");
    properties.put("queued.max.message.chunks", kafkaConfiguration.getQueuedMaxMessageChunks()
        .toString());
    properties.put("rebalance.max.retries", kafkaConfiguration.getRebalanceMaxRetries().toString());
    properties.put("fetch.min.bytes", kafkaConfiguration.getFetchMinBytes().toString());
    properties.put("fetch.wait.max.ms", kafkaConfiguration.getFetchWaitMaxMs().toString());
    properties.put("rebalance.backoff.ms", kafkaConfiguration.getRebalanceBackoffMs().toString());
    properties.put("refresh.leader.backoff.ms", kafkaConfiguration.getRefreshLeaderBackoffMs()
        .toString());
    properties.put("auto.offset.reset", kafkaConfiguration.getAutoOffsetReset());
    properties.put("consumer.timeout.ms", kafkaConfiguration.getConsumerTimeoutMs().toString());
    properties.put("client.id", String.format("%s_%d", pipelineConfiguration.getClientId(), threadNum));
    properties.put("zookeeper.session.timeout.ms", kafkaConfiguration
        .getZookeeperSessionTimeoutMs().toString());
    properties.put("zookeeper.connection.timeout.ms", kafkaConfiguration
        .getZookeeperConnectionTimeoutMs().toString());
    properties
        .put("zookeeper.sync.time.ms", kafkaConfiguration.getZookeeperSyncTimeMs().toString());

    for (String key : properties.stringPropertyNames()) {
      logger.info(KAFKA_CONFIGURATION + " " + key + " = " + properties.getProperty(key));
    }

    return properties;
  }
}
