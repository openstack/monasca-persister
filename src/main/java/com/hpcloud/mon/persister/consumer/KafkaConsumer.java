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

import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;

import com.google.inject.Inject;

import kafka.consumer.KafkaStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class KafkaConsumer {

    private static final String KAFKA_CONFIGURATION = "Kafka configuration:";
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private static final int WAIT_TIME = 10;

    protected final MonPersisterConfiguration configuration;

    private final Integer numThreads;
    private ExecutorService executorService;
    @Inject
    private KafkaStreams kafkaStreams;

    protected abstract Runnable createRunnable(KafkaStream<byte[], byte[]> stream, int threadNumber);
    protected abstract String getStreamName();

    @Inject
    public KafkaConsumer(MonPersisterConfiguration configuration) {

        this.configuration = configuration;

        this.numThreads = configuration.getKafkaConfiguration().getNumThreads();
        logger.info(KAFKA_CONFIGURATION + " numThreads = " + numThreads);
    }

    public void run() {
        List<KafkaStream<byte[], byte[]>> streams = kafkaStreams.getStreams().get(getStreamName());
        executorService = Executors.newFixedThreadPool(numThreads);

        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executorService.submit(createRunnable(stream, threadNumber));
            threadNumber++;
        }
    }

    public void stop() {
        kafkaStreams.stop();
        if (executorService != null) {
            executorService.shutdown();
            try {
              if (!executorService.awaitTermination(WAIT_TIME, TimeUnit.SECONDS)) {
                logger.warn("Did not shut down in %d seconds", WAIT_TIME);
              }
            } catch (InterruptedException e) {
              logger.info("awaitTerminiation interrupted", e);
            }
        }
    }
}
