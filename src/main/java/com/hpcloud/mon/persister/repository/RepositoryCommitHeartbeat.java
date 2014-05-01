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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hpcloud.mon.persister.repository;

import com.google.inject.Inject;
import com.hpcloud.mon.persister.disruptor.AlarmStateHistoryDisruptor;
import com.hpcloud.mon.persister.disruptor.MetricDisruptor;
import com.hpcloud.mon.persister.disruptor.event.AlarmStateTransitionedEventHolder;
import com.hpcloud.mon.persister.disruptor.event.MetricHolder;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryCommitHeartbeat implements Managed {

    private static Logger logger = LoggerFactory.getLogger(RepositoryCommitHeartbeat.class);

    private final MetricDisruptor metricDisruptor;
    private final AlarmStateHistoryDisruptor alarmHistoryDisruptor;
    private final HeartbeatRunnable deduperRunnable;

    @Inject
    public RepositoryCommitHeartbeat(MetricDisruptor metricDisruptor, AlarmStateHistoryDisruptor alarmHistoryDisruptor) {
        this.metricDisruptor = metricDisruptor;
        this.alarmHistoryDisruptor = alarmHistoryDisruptor;
        this.deduperRunnable = new HeartbeatRunnable(metricDisruptor, alarmHistoryDisruptor);
    }

    @Override
    public void start() throws Exception {

        Thread heartbeatThread = new Thread(deduperRunnable);
        heartbeatThread.start();
    }

    @Override
    public void stop() throws Exception {
    }

    private static class HeartbeatRunnable implements Runnable {

        private static final Logger logger = LoggerFactory.getLogger(HeartbeatRunnable.class);
        private final Disruptor metricDisruptor;
        private final Disruptor alarmHistoryDisruptor;

        private HeartbeatRunnable(Disruptor metricDisruptor, Disruptor alarmHistoryDisruptor) {
            this.metricDisruptor = metricDisruptor;
            this.alarmHistoryDisruptor = alarmHistoryDisruptor;
        }

        @Override
        public void run() {
            for (; ; ) {
                try {
                    // Send a heartbeat every second.
                    Thread.sleep(1000);
                    logger.debug("Waking up after sleeping 1 seconds, yawn...");

                    // Send heartbeat
                    logger.debug("Sending heartbeat message");
                    metricDisruptor.publishEvent(new EventTranslator<MetricHolder>() {

                        @Override
                        public void translateTo(MetricHolder event, long sequence) {
                            event.setEnvelope(null);
                        }
                    });
                    alarmHistoryDisruptor.publishEvent(new EventTranslator<AlarmStateTransitionedEventHolder>() {

                        @Override
                        public void translateTo(AlarmStateTransitionedEventHolder event, long sequence) {
                            event.setEvent(null);
                        }
                    });

                } catch (Exception e) {
                    logger.error("Failed to send heartbeat", e);
                }

            }

        }
    }
}
