package com.hpcloud.disruptor.event;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.hpcloud.configuration.MonPersisterConfiguration;
import com.hpcloud.message.MetricMessage;
import com.hpcloud.repository.VerticaMetricRepository;
import com.lmax.disruptor.EventHandler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class MetricMessageEventHandler implements EventHandler<MetricMessageEvent> {

    private static final Logger logger = LoggerFactory.getLogger(MetricMessageEventHandler.class);
    private static final String TENANT_ID = "tenantId";

    private final int ordinal;
    private final int numProcessors;
    private final int batchSize;

    private final SimpleDateFormat simpleDateFormat;

    private long millisSinceLastFlush = System.currentTimeMillis();
    private final long millisBetweenFlushes;
    private final int secondsBetweenFlushes;

    private final VerticaMetricRepository verticaMetricRepository;
    private final MonPersisterConfiguration configuration;

    private final Counter metricCounter = Metrics.newCounter(this.getClass(), "metrics-added-to-batch-counter");
    private final Counter definitionCounter = Metrics.newCounter(this.getClass(), "metric-definitions-added-to-batch-counter");
    private final Counter dimensionCounter = Metrics.newCounter(this.getClass(), "metric-dimensions-added-to-batch-counter");
    private final Meter metricMessageMeter = Metrics.newMeter(this.getClass(), "Metric", "metrics-messages-processed-meter", TimeUnit.SECONDS);
    private final Meter commitMeter = Metrics.newMeter(this.getClass(), "Metric", "commits-executed-meter", TimeUnit.SECONDS);
    private final Timer commitTimer = Metrics.newTimer(this.getClass(), "total-commit-and-flush-timer");

    @Inject
    public MetricMessageEventHandler(VerticaMetricRepository verticaMetricRepository,
                                     MonPersisterConfiguration configuration,
                                     @Assisted("ordinal") int ordinal,
                                     @Assisted("numProcessors") int numProcessors,
                                     @Assisted("batchSize") int batchSize) {

        this.verticaMetricRepository = verticaMetricRepository;
        this.configuration = configuration;
        this.secondsBetweenFlushes = configuration.getMonDeDuperConfiguration().getDedupeRunFrequencySeconds();
        this.millisBetweenFlushes = secondsBetweenFlushes * 1000;

        this.ordinal = ordinal;
        this.numProcessors = numProcessors;
        this.batchSize = batchSize;

        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-0"));


    }

    @Override
    public void onEvent(MetricMessageEvent metricMessageEvent, long sequence, boolean b) throws Exception {

        if (metricMessageEvent.getMetricEnvelope() == null) {
            logger.debug("Received heartbeat message. Checking last flush time.");
            if (millisSinceLastFlush + millisBetweenFlushes < System.currentTimeMillis()) {
                logger.debug("It's been more than " + secondsBetweenFlushes + " seconds since last flush. Flushing staging tables now...");
                flush();
            } else {
                logger.debug("It has not been more than " + secondsBetweenFlushes + " seeconds since last flush. No need to perform flush at this time.");
            }
            return;
        }

        if (((sequence / batchSize) % this.numProcessors) != this.ordinal) {
            return;
        }

        metricMessageMeter.mark();

        logger.debug("Sequence number: " + sequence +
                " Ordinal: " + ordinal +
                " Event: " + metricMessageEvent.getMetricEnvelope().metric);

        MetricMessage metricMessage = metricMessageEvent.getMetricEnvelope().metric;
        Map<String, Object> meta = metricMessageEvent.getMetricEnvelope().meta;
        String tenantId = "";
        if (!meta.containsKey(TENANT_ID)) {
            logger.warn("Failed to find 'tenantId' in message envelope meta data. Metric message may be mal-formed. Setting 'tenantId' to empty string.");
            logger.warn(metricMessage.toString());
            logger.warn("meta" + meta.toString());
        } else {
            tenantId = (String) meta.get(TENANT_ID);
        }

        String stringToHash = metricMessage.getName() + tenantId + metricMessage.getRegion();
        if (metricMessage.getDimensions() != null) {
            for (String name : metricMessage.getDimensions().keySet()) {
                String val = metricMessage.getDimensions().get(name);
                stringToHash += name + val;
            }
        }

        byte[] sha1HashByteArry = DigestUtils.sha(stringToHash.getBytes());

        if (metricMessage.getValue() != null && metricMessage.getTimestamp() != null) {
            String timeStamp = simpleDateFormat.format(new Date(Long.parseLong(metricMessage.getTimestamp()) * 1000));
            Double value = metricMessage.getValue();
            verticaMetricRepository.addToBatchMetrics(sha1HashByteArry, timeStamp, value);
            metricCounter.inc();

        }
        if (metricMessage.getTime_values() != null) {
            for (Double[] timeValuePairs : metricMessage.getTime_values()) {
                String timeStamp = simpleDateFormat.format(new Date((long) (timeValuePairs[0] * 1000)));
                Double value = timeValuePairs[1];
                verticaMetricRepository.addToBatchMetrics(sha1HashByteArry, timeStamp, value);
                metricCounter.inc();

            }
        }

        verticaMetricRepository.addToBatchStagingDefinitions(sha1HashByteArry, metricMessage.getName(), tenantId, metricMessage.getRegion());
        definitionCounter.inc();

        if (metricMessage.getDimensions() != null) {
            for (String name : metricMessage.getDimensions().keySet()) {
                String value = metricMessage.getDimensions().get(name);
                verticaMetricRepository.addToBatchStagingDimensions(sha1HashByteArry, name, value);
                dimensionCounter.inc();
            }
        }

        if (sequence % batchSize == (batchSize - 1)) {
            TimerContext context = commitTimer.time();
            flush();
            context.stop();
            commitMeter.mark();
        }
    }

    private void flush() {
        verticaMetricRepository.flush();
        millisSinceLastFlush = System.currentTimeMillis();
    }
}

