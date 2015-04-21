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

package monasca.persister.consumer;

import monasca.persister.configuration.KafkaConfig;
import monasca.persister.configuration.PersisterConfig;
import monasca.persister.configuration.PipelineConfig;

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
  private final String threadId;

  @Inject
  public KafkaChannel(
      PersisterConfig configuration,
      @Assisted PipelineConfig pipelineConfig,
      @Assisted String threadId) {

    this.topic = pipelineConfig.getTopic();
    this.threadId = threadId;
    Properties kafkaProperties =
        createKafkaProperties(configuration.getKafkaConfig(), pipelineConfig);
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

  private Properties createKafkaProperties(KafkaConfig kafkaConfig,
      final PipelineConfig pipelineConfig) {
    Properties properties = new Properties();

    properties.put("group.id", pipelineConfig.getGroupId());
    properties.put("zookeeper.connect", kafkaConfig.getZookeeperConnect());
    properties.put("consumer.id",
        String.format("%s_%s", pipelineConfig.getConsumerId(), this.threadId));
    properties.put("socket.timeout.ms", kafkaConfig.getSocketTimeoutMs().toString());
    properties.put("socket.receive.buffer.bytes", kafkaConfig.getSocketReceiveBufferBytes()
        .toString());
    properties.put("fetch.message.max.bytes", kafkaConfig.getFetchMessageMaxBytes()
        .toString());
    // Set auto commit to false because the persister is going to explicitly commit
    properties.put("auto.commit.enable", "false");
    properties.put("queued.max.message.chunks", kafkaConfig.getQueuedMaxMessageChunks()
        .toString());
    properties.put("rebalance.max.retries", kafkaConfig.getRebalanceMaxRetries().toString());
    properties.put("fetch.min.bytes", kafkaConfig.getFetchMinBytes().toString());
    properties.put("fetch.wait.max.ms", kafkaConfig.getFetchWaitMaxMs().toString());
    properties.put("rebalance.backoff.ms", kafkaConfig.getRebalanceBackoffMs().toString());
    properties.put("refresh.leader.backoff.ms", kafkaConfig.getRefreshLeaderBackoffMs()
        .toString());
    properties.put("auto.offset.reset", kafkaConfig.getAutoOffsetReset());
    properties.put("consumer.timeout.ms", kafkaConfig.getConsumerTimeoutMs().toString());
    properties.put("client.id", String.format("%s_%s", pipelineConfig.getClientId(), threadId));
    properties.put("zookeeper.session.timeout.ms", kafkaConfig
        .getZookeeperSessionTimeoutMs().toString());
    properties.put("zookeeper.connection.timeout.ms", kafkaConfig
        .getZookeeperConnectionTimeoutMs().toString());
    properties
        .put("zookeeper.sync.time.ms", kafkaConfig.getZookeeperSyncTimeMs().toString());

    for (String key : properties.stringPropertyNames()) {
      logger.info("[{}]: " + KAFKA_CONFIGURATION + " " + key + " = " + properties.getProperty(key), threadId);
    }

    return properties;
  }
}
