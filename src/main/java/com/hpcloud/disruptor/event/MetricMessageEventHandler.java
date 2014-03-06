package com.hpcloud.disruptor.event;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
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
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class MetricMessageEventHandler implements EventHandler<MetricMessageEvent> {

    private static final Logger logger = LoggerFactory.getLogger(MetricMessageEventHandler.class);

    private final int ordinal;
    private final int numProcessors;
    private final int batchSize;

    private final SimpleDateFormat simpleDateFormat;

    private final VerticaMetricRepository verticaMetricRepository;
    private final Counter metricCounter = Metrics.newCounter(this.getClass(), "metrics-added-to-batch-counter");
    private final Counter definitionCounter = Metrics.newCounter(this.getClass(), "metric-definitions-added-to-batch-counter");
    private final Counter dimensionCounter = Metrics.newCounter(this.getClass(), "metric-dimensions-added-to-batch-counter");
    private final Meter metricMessageMeter = Metrics.newMeter(this.getClass(), "Metric", "metrics-messages-processed-meter", TimeUnit.SECONDS);
    private final Meter commitMeter = Metrics.newMeter(this.getClass(), "Metric", "commits-executed-meter", TimeUnit.SECONDS);
    private final Timer commitTimer = Metrics.newTimer(this.getClass(), "commits-executed-timer");

    @Inject
    public MetricMessageEventHandler(VerticaMetricRepository verticaMetricRepository,
                                     @Assisted("ordinal") int ordinal,
                                     @Assisted("numProcessors") int numProcessors,
                                     @Assisted("batchSize") int batchSize) {

        this.verticaMetricRepository = verticaMetricRepository;
        this.ordinal = ordinal;
        this.numProcessors = numProcessors;
        this.batchSize = batchSize;

        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-0"));

    }

    @Override
    public void onEvent(MetricMessageEvent metricMessageEvent, long sequence, boolean b) throws Exception {

        if (((sequence / batchSize) % this.numProcessors) != this.ordinal) {
            return;
        }

        metricMessageMeter.mark();

        logger.debug("Sequence number: " + sequence +
                " Ordinal: " + ordinal +
                " Event: " + metricMessageEvent.getMetricMessage());

        MetricMessage metricMessage = metricMessageEvent.getMetricMessage();

        String stringToHash = metricMessage.getName() + metricMessage.getRegion() + metricMessage.getTenant();
        if (metricMessage.getDimensions() != null) {
            for (String name : metricMessage.getDimensions().keySet()) {
                String val = metricMessage.getDimensions().get(name);
                stringToHash += name + val;
            }
        }

        byte[] sha1HashByteArry = DigestUtils.sha(stringToHash.getBytes());

        if (metricMessage.getValue() != null && metricMessage.getTimeStamp() != null) {
            String timeStamp = simpleDateFormat.format(new Date(Long.parseLong(metricMessage.getTimeStamp()) * 1000));
            Double value = metricMessage.getValue();
            verticaMetricRepository.addToBatchMetrics(sha1HashByteArry, timeStamp, metricMessage.getValue());
            metricCounter.inc();

        }
        if (metricMessage.getTimeValues() != null) {
            for (Double[] timeValuePairs : metricMessage.getTimeValues()) {
                String timeStamp = simpleDateFormat.format(new Date((long) (timeValuePairs[0] * 1000)));
                Double value = timeValuePairs[1];
                verticaMetricRepository.addToBatchMetrics(sha1HashByteArry, timeStamp, value);
                metricCounter.inc();

            }
        }

        verticaMetricRepository.addToBatchStagingDefinitions(sha1HashByteArry, metricMessage.getName(), metricMessage.getTenant(), metricMessage.getRegion());
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
            verticaMetricRepository.commitBatch();
            context.stop();
            commitMeter.mark();
        }


    }
}
