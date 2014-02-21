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

    private final String topic;
    private final Integer numThreads;
    private final ConsumerConnector consumerConnector;
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
        properties.put("socket.timeout.ms", kafkaConfiguration.socketTimeoutMs.toString());
        properties.put("socket.receive.buffer.bytes", kafkaConfiguration.socketReceiveBufferBytes.toString());
        properties.put("fetch.message.max.bytes", kafkaConfiguration.fetchMessageMaxBytes.toString());
        properties.put("auto.commit.enable", kafkaConfiguration.autoCommitEnable.toString());
        properties.put("auto.commit.interval.ms", kafkaConfiguration.autoCommitIntervalMs.toString());
        properties.put("queued.max.message.chunks", kafkaConfiguration.queuedMaxMessageChunks.toString());
        properties.put("rebalance.max.retries", kafkaConfiguration.rebalanceMaxRetries.toString());
        properties.put("fetch.min.bytes", kafkaConfiguration.fetchMinBytes.toString());
        properties.put("fetch.wait.max.ms", kafkaConfiguration.fetchWaitMaxMs.toString());
        properties.put("rebalance.backoff.ms", kafkaConfiguration.rebalanceBackoffMs.toString());
        properties.put("refresh.leader.backoff.ms", kafkaConfiguration.refreshLeaderBackoffMs.toString());
        properties.put("auto.offset.reset", kafkaConfiguration.autoOffsetReset);
        properties.put("consumer.timeout.ms", kafkaConfiguration.consumerTimeoutMs.toString());
        properties.put("client.id", kafkaConfiguration.clientId);
        properties.put("zookeeper.session.timeout.ms", kafkaConfiguration.zookeeperSessionTimeoutMs.toString());
        properties.put("zookeeper.connection.timeout.ms", kafkaConfiguration.zookeeperSessionTimeoutMs.toString());
        properties.put("zookeeper.sync.time.ms", kafkaConfiguration.zookeeperSyncTimeMs.toString());
        return properties;
    }

}
