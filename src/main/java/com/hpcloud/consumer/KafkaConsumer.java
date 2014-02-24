package com.hpcloud.consumer;

import com.google.inject.Inject;
import com.hpcloud.configuration.KafkaConfiguration;
import com.hpcloud.configuration.MonPersisterConfiguration;
import com.hpcloud.disruptor.DisruptorFactory;
import com.lmax.disruptor.dsl.Disruptor;
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
    private final Disruptor disruptor;

    @Inject
    public KafkaConsumer(MonPersisterConfiguration configuration, DisruptorFactory disruptorFactory) {

        this.topic = configuration.getKafkaConfiguration().getTopic();
        this.numThreads = configuration.getKafkaConfiguration().getNumThreads();
        this.disruptor = disruptorFactory.create();
        Properties kafkaProperties = createKafkaProperties(configuration.getKafkaConfiguration());
        ConsumerConfig consumerConfig = createConsumerConfig(kafkaProperties);
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
            executorService.submit(new KafkaConsumerRunnableBasic(stream, threadNumber, disruptor));
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
        properties.put("group.id", kafkaConfiguration.getGroupId());
        properties.put("zookeeper.connect", kafkaConfiguration.getZookeeperConnect());
        properties.put("consumer.id", kafkaConfiguration.getConsumerId());
        properties.put("socket.timeout.ms", kafkaConfiguration.getSocketTimeoutMs().toString());
        properties.put("socket.receive.buffer.bytes", kafkaConfiguration.getSocketReceiveBufferBytes().toString());
        properties.put("fetch.message.max.bytes", kafkaConfiguration.getFetchMessageMaxBytes().toString());
        properties.put("auto.commit.enable", kafkaConfiguration.getAutoCommitEnable().toString());
        properties.put("auto.commit.interval.ms", kafkaConfiguration.getAutoCommitIntervalMs().toString());
        properties.put("queued.max.message.chunks", kafkaConfiguration.getQueuedMaxMessageChunks().toString());
        properties.put("rebalance.max.retries", kafkaConfiguration.getRebalanceMaxRetries().toString());
        properties.put("fetch.min.bytes", kafkaConfiguration.getFetchMinBytes().toString());
        properties.put("fetch.wait.max.ms", kafkaConfiguration.getFetchWaitMaxMs().toString());
        properties.put("rebalance.backoff.ms", kafkaConfiguration.getRebalanceBackoffMs().toString());
        properties.put("refresh.leader.backoff.ms", kafkaConfiguration.getRefreshLeaderBackoffMs().toString());
        properties.put("auto.offset.reset", kafkaConfiguration.getAutoOffsetReset());
        properties.put("consumer.timeout.ms", kafkaConfiguration.getConsumerTimeoutMs().toString());
        properties.put("client.id", kafkaConfiguration.getClientId());
        properties.put("zookeeper.session.timeout.ms", kafkaConfiguration.getZookeeperSessionTimeoutMs().toString());
        properties.put("zookeeper.connection.timeout.ms", kafkaConfiguration.getZookeeperConnectionTimeoutMs().toString());
        properties.put("zookeeper.sync.time.ms", kafkaConfiguration.getZookeeperSyncTimeMs().toString());
        return properties;
    }

}
