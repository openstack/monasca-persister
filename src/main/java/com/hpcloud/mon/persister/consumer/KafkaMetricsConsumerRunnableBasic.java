/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.hpcloud.mon.persister.consumer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.hpcloud.mon.persister.disruptor.MetricDisruptor;
import com.hpcloud.mon.persister.disruptor.event.MetricHolder;
import com.hpcloud.mon.common.model.metric.MetricEnvelope;
import com.lmax.disruptor.EventTranslator;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMetricsConsumerRunnableBasic implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMetricsConsumerRunnableBasic.class);
    private final KafkaStream stream;
    private final int threadNumber;
    private final MetricDisruptor disruptor;
    private final ObjectMapper objectMapper;

    @Inject
    public KafkaMetricsConsumerRunnableBasic(MetricDisruptor disruptor,
                                             @Assisted KafkaStream stream,
                                             @Assisted int threadNumber) {
        this.stream = stream;
        this.threadNumber = threadNumber;
        this.disruptor = disruptor;
        this.objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    }

    @SuppressWarnings("unchecked")
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {

            final String s = new String(it.next().message());

            logger.debug("Thread " + threadNumber + ": " + s);

            try {
                final MetricEnvelope[] envelopes = objectMapper.readValue(s, MetricEnvelope[].class);

                for (final MetricEnvelope envelope : envelopes) {

                    logger.debug(envelope.toString());

                    disruptor.publishEvent(new EventTranslator<MetricHolder>() {
                        @Override
                        public void translateTo(MetricHolder event, long sequence) {
                            event.setEnvelope(envelope);
                        }

                    });
                }
            } catch (Exception e) {
                logger.error("Failed to deserialize JSON message and place on disruptor queue: " + s, e);
            }
        }
        logger.debug("Shutting down Thread: " + threadNumber);
    }
}
