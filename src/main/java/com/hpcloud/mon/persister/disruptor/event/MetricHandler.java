package com.hpcloud.mon.persister.disruptor.event;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.hpcloud.mon.common.model.metric.Metric;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.repository.Sha1HashId;
import com.hpcloud.mon.persister.repository.VerticaMetricRepository;
import com.lmax.disruptor.EventHandler;
import io.dropwizard.setup.Environment;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import static com.hpcloud.mon.persister.repository.VerticaMetricsConstants.MAX_COLUMN_LENGTH;
import static com.hpcloud.mon.persister.repository.VerticaMetricsConstants.MAX_TENANT_ID_LENGTH;

public class MetricHandler implements EventHandler<MetricHolder> {

    private static final Logger logger = LoggerFactory.getLogger(MetricHandler.class);
    private static final String TENANT_ID = "tenantId";
    private static final String REGION = "region";

    private final int ordinal;
    private final int numProcessors;
    private final int batchSize;

    private final SimpleDateFormat simpleDateFormat;

    private long millisSinceLastFlush = System.currentTimeMillis();
    private final long millisBetweenFlushes;
    private final int secondsBetweenFlushes;

    private final VerticaMetricRepository verticaMetricRepository;
    private final MonPersisterConfiguration configuration;
    private final Environment environment;

    private final Counter metricCounter;
    private final Counter definitionCounter;
    private final Counter dimensionCounter;
    private final Counter definitionDimensionsCounter;
    private final Meter metricMeter;
    private final Meter commitMeter;
    private final Timer commitTimer;

    @Inject
    public MetricHandler(VerticaMetricRepository verticaMetricRepository,
                         MonPersisterConfiguration configuration,
                         Environment environment,
                         @Assisted("ordinal") int ordinal,
                         @Assisted("numProcessors") int numProcessors,
                         @Assisted("batchSize") int batchSize) {

        this.verticaMetricRepository = verticaMetricRepository;
        this.configuration = configuration;
        this.environment = environment;
        this.metricCounter = this.environment.metrics().counter(this.getClass().getName() + "." + "metrics-added-to-batch-counter");
        this.definitionCounter = this.environment.metrics().counter(this.getClass().getName() + "." + "metric-definitions-added-to-batch-counter");
        this.dimensionCounter = this.environment.metrics().counter(this.getClass().getName() + "." + "metric-dimensions-added-to-batch-counter");
        this.definitionDimensionsCounter = this.environment.metrics().counter(this.getClass().getName() + "." + "metric-definition-dimensions-added-to-batch-counter");
        this.metricMeter = this.environment.metrics().meter(this.getClass().getName() + "." + "metrics-messages-processed-meter");
        this.commitMeter = this.environment.metrics().meter(this.getClass().getName() + "." + "commits-executed-meter");
        this.commitTimer = this.environment.metrics().timer(this.getClass().getName() + "." + "total-commit-and-flush-timer");

        this.secondsBetweenFlushes = configuration.getMonDeDuperConfiguration().getDedupeRunFrequencySeconds();
        this.millisBetweenFlushes = secondsBetweenFlushes * 1000;

        this.ordinal = ordinal;
        this.numProcessors = numProcessors;
        this.batchSize = batchSize;

        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-0"));


    }

    @Override
    public void onEvent(MetricHolder metricEvent, long sequence, boolean b) throws Exception {

        if (metricEvent.getMetricEnvelope() == null) {
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

        metricMeter.mark();

        Metric metric = metricEvent.getMetricEnvelope().metric;
        Map<String, Object> meta = metricEvent.getMetricEnvelope().meta;

        logger.debug("sequence number: " + sequence);
        logger.debug("ordinal: " + ordinal);
        logger.debug("metric: " + metric.toString());
        logger.debug("meta: " + meta.toString());

        String tenantId = "";
        if (meta.containsKey(TENANT_ID)) {
            tenantId = (String) meta.get(TENANT_ID);
        } else {
            logger.warn("Failed to find tenantId in message envelope meta data. Metric message may be mal-formed. Setting tenantId to empty string.");
            logger.warn("metric: " + metric.toString());
            logger.warn("meta: " + meta.toString());
        }

        String region = "";
        if (meta.containsKey(REGION)) {
            region = (String) meta.get(REGION);
        } else {
            logger.warn("Failed to find region in message envelope meta data. Metric message may be mal-formed. Setting region to empty string.");
            logger.warn("metric: " + metric.toString());
            logger.warn("meta: " + meta.toString());
        }

        // Add the definition to the batch.
        String definitionIdStringToHash = trunc(metric.getName(), MAX_COLUMN_LENGTH) + trunc(tenantId, MAX_TENANT_ID_LENGTH) + trunc(region, MAX_COLUMN_LENGTH);
        byte[] definitionIdSha1Hash = DigestUtils.sha(definitionIdStringToHash);
        Sha1HashId definitionSha1HashId = new Sha1HashId((definitionIdSha1Hash));
        verticaMetricRepository.addToBatchStagingDefinitions(definitionSha1HashId, trunc(metric.getName(), MAX_COLUMN_LENGTH), trunc(tenantId, MAX_TENANT_ID_LENGTH), trunc(region, MAX_COLUMN_LENGTH));
        definitionCounter.inc();

        // Calculate dimensions sha1 hash id.
        String dimensionIdStringToHash = "";
        if (metric.getDimensions() != null) {
            // Sort the dimensions on name and value.
            TreeMap<String, String> dimensionTreeMap = new TreeMap<>(metric.getDimensions());
            for (String dimensionName : dimensionTreeMap.keySet()) {
                if (dimensionName != null && !dimensionName.isEmpty()) {
                    String dimensionValue = dimensionTreeMap.get(dimensionName);
                    if (dimensionValue != null && !dimensionValue.isEmpty()) {
                        dimensionIdStringToHash += trunc(dimensionName, MAX_COLUMN_LENGTH) + trunc(dimensionValue, MAX_COLUMN_LENGTH);
                    }
                }
            }
        }

        byte[] dimensionIdSha1Hash = DigestUtils.sha(dimensionIdStringToHash);
        Sha1HashId dimensionsSha1HashId = new Sha1HashId(dimensionIdSha1Hash);

        // Add the dimension name/values to the batch.
        if (metric.getDimensions() != null) {
            TreeMap<String, String> dimensionTreeMap = new TreeMap<>(metric.getDimensions());
            for (String dimensionName : dimensionTreeMap.keySet()) {
                if (dimensionName != null && !dimensionName.isEmpty()) {
                    String dimensionValue = dimensionTreeMap.get(dimensionName);
                    if (dimensionValue != null && !dimensionValue.isEmpty()) {
                        verticaMetricRepository.addToBatchStagingDimensions(dimensionsSha1HashId, trunc(dimensionName, MAX_COLUMN_LENGTH), trunc(dimensionValue, MAX_COLUMN_LENGTH));
                        dimensionCounter.inc();
                    }
                }
            }
        }

        // Add the definition dimensions to the batch.
        String definitionDimensionsIdStringToHash = definitionSha1HashId.toHexString() + dimensionsSha1HashId.toHexString();
        byte[] definitionDimensionsIdSha1Hash = DigestUtils.sha(definitionDimensionsIdStringToHash);
        Sha1HashId definitionDimensionsSha1HashId = new Sha1HashId(definitionDimensionsIdSha1Hash);
        verticaMetricRepository.addToBatchStagingdefinitionDimensions(definitionDimensionsSha1HashId, definitionSha1HashId, dimensionsSha1HashId);
        definitionDimensionsCounter.inc();

        // Add the measurements to the batch.
        if (metric.getTimeValues() != null) {
            for (double[] timeValuePairs : metric.getTimeValues()) {
                String timeStamp = simpleDateFormat.format(new Date((long) (timeValuePairs[0] * 1000)));
                double value = timeValuePairs[1];
                verticaMetricRepository.addToBatchMetrics(definitionDimensionsSha1HashId, timeStamp, value);
                metricCounter.inc();
            }
        } else {
            String timeStamp = simpleDateFormat.format(new Date(metric.getTimestamp() * 1000));
            double value = metric.getValue();
            verticaMetricRepository.addToBatchMetrics(definitionDimensionsSha1HashId, timeStamp, value);
            metricCounter.inc();
        }

        if (sequence % batchSize == (batchSize - 1)) {
            Timer.Context context = commitTimer.time();
            flush();
            context.stop();
            commitMeter.mark();
        }

    }

    private void flush() {
        verticaMetricRepository.flush();
        millisSinceLastFlush = System.currentTimeMillis();
    }

    private String trunc(String s, int l) {

        if (s == null) {
            return "";
        } else if (s.length() <= l) {
            return s;
        } else {
            String r = s.substring(0, l);
            logger.warn("Input string exceeded max column length. Truncating input string {} to {} chars", s, l);
            logger.warn("Resulting string {}", r);
            return r;
        }

    }
}

