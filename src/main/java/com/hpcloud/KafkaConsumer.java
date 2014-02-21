package com.hpcloud;

import com.google.inject.Inject;
import com.yammer.dropwizard.config.Environment;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumer {

    private static Logger logger = LoggerFactory.getLogger(MonConsumer.class);

    private final ConsumerConnector consumerConnector;

    private final String topic;
    private final Integer numThreads;
    private ExecutorService executorService;

    @Inject
    public KafkaConsumer(MonPersisterConfiguration configuration, Environment environment) {

        Properties kafkaProperties = createKafkaProperties(configuration.getKafkaConfiguration());
        ConsumerConfig consumerConfig = createConsumerConfig(kafkaProperties);
        this.topic = configuration.getKafkaConfiguration().topic;
        this.numThreads = configuration.getKafkaConfiguration().numThreads;
        this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        executorService = Executors.newFixedThreadPool(numThreads);

        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executorService.submit(new KafkaConsumerRunnableBasic(stream, threadNumber));
        }

    }

    public void stop() {
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private ConsumerConfig createConsumerConfig(Properties kafkaProperties) {
        return new ConsumerConfig(kafkaProperties);
    }

    private Properties createKafkaProperties(KafkaConfiguration kafkaConfiguration) {
        Properties properties = new Properties();
        properties.put("group.id", kafkaConfiguration.groupId);
        properties.put("zookeeper.connect", kafkaConfiguration.zookeeperConnect);
        properties.put("consumer.id", kafkaConfiguration.consumerId);
        properties.put("socket.timeout.ms", kafkaConfiguration.socketTimeoutMs);
        properties.put("socket.receive.buffer.bytes", kafkaConfiguration.socketReceiveBufferBytes);
        properties.put("fetch.message.max.bytes", kafkaConfiguration.fetchMessageMaxBytes);
        properties.put("auto.commit.enable", kafkaConfiguration.autoCommitEnable);
        properties.put("auto.commit.interval.ms", kafkaConfiguration.autoCommitIntervalMs);
        properties.put("queued.max.message.chunks", kafkaConfiguration.queuedMaxMessageChunks);
        properties.put("rebalance.max.retries", kafkaConfiguration.rebalanceMaxRetries);
        properties.put("fetch.min.bytes", kafkaConfiguration.fetchMinBytes);
        properties.put("fetch.wait.max.ms", kafkaConfiguration.fetchWaitMaxMs);
        properties.put("rebalance.backoff.ms", kafkaConfiguration.rebalanceBackoffMs);
        properties.put("refresh.leader.backoff.ms", kafkaConfiguration.refreshLeaderBackoffMs);
        properties.put("auto.offset.reset", kafkaConfiguration.autoOffsetReset);
        properties.put("consumer.timeout.ms", kafkaConfiguration.consumerTimeoutMs);
        properties.put("client.id", kafkaConfiguration.clientId);
        properties.put("zookeeper.session.timeout.ms", kafkaConfiguration.zookeeperSessionTimeoutMs);
        properties.put("zookeeper.connection.timeout.ms", kafkaConfiguration.zookeeperSessionTimeoutMs);
        properties.put("zookeeper.sync.time.ms", kafkaConfiguration.zookeeperSyncTimeMs);
        return properties;
    }

}
