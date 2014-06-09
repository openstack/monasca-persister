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

public class KafkaStreams {
    private static final String KAFKA_CONFIGURATION = "Kafka configuration:";
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreams.class);

    private final ConsumerConnector consumerConnector;
    private final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;

    public KafkaStreams(MonPersisterConfiguration configuration) {
        Properties kafkaProperties = createKafkaProperties(configuration.getKafkaConfiguration());
        ConsumerConfig consumerConfig = createConsumerConfig(kafkaProperties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<>();
        Integer numThreads = configuration.getKafkaConfiguration().getNumThreads();
        topicCountMap.put("metrics", (int) numThreads);
        topicCountMap.put("alarm-state-transitions", (int) numThreads);
        consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    }

    public final Map<String, List<KafkaStream<byte[], byte[]>>> getStreams() {
        return consumerMap;
    }

    private ConsumerConfig createConsumerConfig(Properties kafkaProperties) {
        return new ConsumerConfig(kafkaProperties);
    }

    private Properties createKafkaProperties(KafkaConfiguration metricsKafkaConfiguration) {
        Properties properties = new Properties();

        properties.put("group.id", metricsKafkaConfiguration.getGroupId());
        properties.put("zookeeper.connect", metricsKafkaConfiguration.getZookeeperConnect());
        properties.put("consumer.id", metricsKafkaConfiguration.getConsumerId());
        properties.put("socket.timeout.ms", metricsKafkaConfiguration.getSocketTimeoutMs().toString());
        properties.put("socket.receive.buffer.bytes", metricsKafkaConfiguration.getSocketReceiveBufferBytes().toString());
        properties.put("fetch.message.max.bytes", metricsKafkaConfiguration.getFetchMessageMaxBytes().toString());
        properties.put("auto.commit.enable", metricsKafkaConfiguration.getAutoCommitEnable().toString());
        properties.put("auto.commit.interval.ms", metricsKafkaConfiguration.getAutoCommitIntervalMs().toString());
        properties.put("queued.max.message.chunks", metricsKafkaConfiguration.getQueuedMaxMessageChunks().toString());
        properties.put("rebalance.max.retries", metricsKafkaConfiguration.getRebalanceMaxRetries().toString());
        properties.put("fetch.min.bytes", metricsKafkaConfiguration.getFetchMinBytes().toString());
        properties.put("fetch.wait.max.ms", metricsKafkaConfiguration.getFetchWaitMaxMs().toString());
        properties.put("rebalance.backoff.ms", metricsKafkaConfiguration.getRebalanceBackoffMs().toString());
        properties.put("refresh.leader.backoff.ms", metricsKafkaConfiguration.getRefreshLeaderBackoffMs().toString());
        properties.put("auto.offset.reset", metricsKafkaConfiguration.getAutoOffsetReset());
        properties.put("consumer.timeout.ms", metricsKafkaConfiguration.getConsumerTimeoutMs().toString());
        properties.put("client.id", metricsKafkaConfiguration.getClientId());
        properties.put("zookeeper.session.timeout.ms", metricsKafkaConfiguration.getZookeeperSessionTimeoutMs().toString());
        properties.put("zookeeper.connection.timeout.ms", metricsKafkaConfiguration.getZookeeperConnectionTimeoutMs().toString());
        properties.put("zookeeper.sync.time.ms", metricsKafkaConfiguration.getZookeeperSyncTimeMs().toString());

        for (String key : properties.stringPropertyNames()) {
            logger.info(KAFKA_CONFIGURATION + " " + key + " = " + properties.getProperty(key));
        }

        return properties;
    }
}
