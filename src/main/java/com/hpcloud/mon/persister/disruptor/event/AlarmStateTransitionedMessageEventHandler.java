package com.hpcloud.mon.persister.disruptor.event;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.common.event.AlarmStateTransitionedEvent;
import com.hpcloud.mon.persister.repository.VerticaAlarmStateHistoryRepository;
import com.lmax.disruptor.EventHandler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class AlarmStateTransitionedMessageEventHandler implements EventHandler<AlarmStateTransitionedMessageEvent> {

    private static final Logger logger = LoggerFactory.getLogger(AlarmStateTransitionedMessageEventHandler.class);
    private final int ordinal;
    private final int numProcessors;
    private final int batchSize;

    private long millisSinceLastFlush = System.currentTimeMillis();
    private final long millisBetweenFlushes;
    private final int secondsBetweenFlushes;

    private final VerticaAlarmStateHistoryRepository repository;
    private final MonPersisterConfiguration configuration;

    private final Counter batchCounter = Metrics.newCounter(this.getClass(), "alarm-added-to-batch-batchCounter");
    private final Meter processedMeter = Metrics.newMeter(this.getClass(), "Alarm", "alarm-messages-processed-processedMeter", TimeUnit.SECONDS);
    private final Meter commitMeter = Metrics.newMeter(this.getClass(), "Metric", "commits-executed-processedMeter", TimeUnit.SECONDS);
    private final Timer commitTimer = Metrics.newTimer(this.getClass(), "total-commit-and-flush-timer");

    @Inject
    public AlarmStateTransitionedMessageEventHandler(VerticaAlarmStateHistoryRepository repository,
                                                     MonPersisterConfiguration configuration,
                                                     @Assisted("ordinal") int ordinal,
                                                     @Assisted("numProcessors") int numProcessors,
                                                     @Assisted("batchSize") int batchSize) {

        this.repository = repository;
        this.configuration = configuration;
        this.secondsBetweenFlushes = configuration.getMonDeDuperConfiguration().getDedupeRunFrequencySeconds();
        this.millisBetweenFlushes = secondsBetweenFlushes * 1000;

        this.ordinal = ordinal;
        this.numProcessors = numProcessors;
        this.batchSize = batchSize;
    }

    @Override
    public void onEvent(AlarmStateTransitionedMessageEvent event, long sequence, boolean b) throws Exception {

        if (event.getMessage() == null) {
            logger.debug("Received heartbeat message. Checking last flush time.");
            if (millisSinceLastFlush + millisBetweenFlushes < System.currentTimeMillis()) {
                logger.debug("It's been more than " + secondsBetweenFlushes + " seconds since last flush. Flushing staging tables now...");
                flush();
            } else {
                logger.debug("It has not been more than " + secondsBetweenFlushes + " seconds since last flush. No need to perform flush at this time.");
            }
            return;
        }

        if (((sequence / batchSize) % this.numProcessors) != this.ordinal) {
            return;
        }

        processedMeter.mark();

        logger.debug("Sequence number: " + sequence +
                " Ordinal: " + ordinal +
                " Event: " + event.getMessage());

        AlarmStateTransitionedEvent message = event.getMessage();
        repository.addToBatch(message);

        if (sequence % batchSize == (batchSize - 1)) {
            TimerContext context = commitTimer.time();
            flush();
            context.stop();
            commitMeter.mark();
        }
    }

    private void flush() {
        repository.flush();
        millisSinceLastFlush = System.currentTimeMillis();
    }
}

