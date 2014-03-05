package com.hpcloud.event;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.hpcloud.message.MetricMessage;
import com.hpcloud.repository.VerticaMetricRepository;
import com.lmax.disruptor.EventHandler;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class MetricMessageEventHandler implements EventHandler<MetricMessageEvent> {

    private static final Logger logger = LoggerFactory.getLogger(MetricMessageEventHandler.class);

    private final int ordinal;
    private final int numProcessors;
    private final int batchSize;

    private final SimpleDateFormat simpleDateFormat;

    VerticaMetricRepository verticaMetricRepository;

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

        logger.info("Sequence number: " + sequence +
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

        }
        if (metricMessage.getTimeValues() != null) {
            for (Double[] timeValuePairs : metricMessage.getTimeValues()) {
                String timeStamp = simpleDateFormat.format(new Date((long) (timeValuePairs[0] * 1000)));
                Double value = timeValuePairs[1];
                verticaMetricRepository.addToBatchMetrics(sha1HashByteArry, timeStamp, value);

            }
        }

        verticaMetricRepository.addToBatchStagingDefinitions(sha1HashByteArry, metricMessage.getName(), metricMessage.getTenant(), metricMessage.getRegion());

        if (metricMessage.getDimensions() != null) {
            for (String name : metricMessage.getDimensions().keySet()) {
                String value = metricMessage.getDimensions().get(name);
                verticaMetricRepository.addToBatchStagingDimensions(sha1HashByteArry, name, value);
            }
        }
        if (sequence % batchSize == (batchSize - 1)) {
            verticaMetricRepository.commitBatch();
        }


    }
}
