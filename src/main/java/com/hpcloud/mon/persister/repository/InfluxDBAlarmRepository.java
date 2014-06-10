package com.hpcloud.mon.persister.repository;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import com.hpcloud.mon.common.event.AlarmStateTransitionedEvent;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import io.dropwizard.setup.Environment;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class InfluxDBAlarmRepository implements AlarmRepository {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBAlarmRepository.class);
    private static final String ALARM_STATE_HISTORY_NAME = "alarm_state_history";
    private static final int ALARM_STATE_HISTORY_NUM_COLUMNS = 7;

    private final String[] colNamesStringArry = {"tenant_id",
            "alarm_id",
            "old_state",
            "new_state",
            "reason",
            "reason_data",
            "time_stamp"};

    private static final int ALARM_STATE_HISTORY_COLUMN_NUMBER = 7;

    private final MonPersisterConfiguration configuration;
    private final Environment environment;
    private final InfluxDB influxDB;

    private List<AlarmStateTransitionedEvent> alarmStateTransitionedEventList = new LinkedList<>();

    private final Timer flushTimer;
    public final Meter alarmStateHistoryMeter;

    @Inject
    public InfluxDBAlarmRepository(MonPersisterConfiguration configuration,
                                   Environment environment) {
        this.configuration = configuration;
        this.environment = environment;
        influxDB = InfluxDBFactory.connect(configuration.getInfluxDBConfiguration().getUrl(),
                configuration.getInfluxDBConfiguration().getUser(),
                configuration.getInfluxDBConfiguration().getPassword());

        this.flushTimer = this.environment.metrics().timer(this.getClass().getName() + "." + "flush-timer");
        this.alarmStateHistoryMeter = this.environment.metrics().meter(this.getClass().getName() + "." + "alarm_state_history-meter");


    }

    @Override
    public void addToBatch(AlarmStateTransitionedEvent alarmStateTransitionedEvent) {
        alarmStateTransitionedEventList.add(alarmStateTransitionedEvent);
        this.alarmStateHistoryMeter.mark();
    }

    @Override
    public void flush() {

        try {

            if (this.alarmStateTransitionedEventList.isEmpty()) {
                logger.debug("There are no alarm state transition events to be written to the influxDB");
                logger.debug("Returning from flush");
                return;
            }

            long startTime = System.currentTimeMillis();
            Timer.Context context = flushTimer.time();

            Serie serie = new Serie(ALARM_STATE_HISTORY_NAME);
            logger.debug("Created serie: " + serie.getName());

            serie.setColumns(this.colNamesStringArry);

            if (logger.isDebugEnabled()) {
                logger.debug("Added array of column names to serie");
                StringBuffer sb = new StringBuffer();
                boolean first = true;
                for (String colName : serie.getColumns()) {
                    if (first) {
                        first = false;
                    } else {
                        sb.append(",");
                    }
                    sb.append(colName);
                }
                logger.debug("Array of column names: [" + sb.toString() + "]");
            }

            Object[][] colValsObjectArry = new Object[this.alarmStateTransitionedEventList.size()][ALARM_STATE_HISTORY_NUM_COLUMNS];
            int i = 0;
            for (AlarmStateTransitionedEvent alarmStateTransitionedEvent : alarmStateTransitionedEventList) {
                int j = 0;
                colValsObjectArry[i][j++] = alarmStateTransitionedEvent.tenantId;
                colValsObjectArry[i][j++] = alarmStateTransitionedEvent.alarmId;
                colValsObjectArry[i][j++] = alarmStateTransitionedEvent.oldState;
                colValsObjectArry[i][j++] = alarmStateTransitionedEvent.newState;
                colValsObjectArry[i][j++] = alarmStateTransitionedEvent.stateChangeReason;
                colValsObjectArry[i][j++] = "{}";
                colValsObjectArry[i][j++] = alarmStateTransitionedEvent.timestamp;
                i++;
            }

            serie.setPoints(colValsObjectArry);

            if (logger.isDebugEnabled()) {
                logger.debug("Added array of array of column values to serie");
                int outerIdx = 0;
                for (Object[] colValArry : serie.getPoints()) {
                    StringBuffer sb = new StringBuffer();
                    boolean first = true;
                    for (Object colVal : colValArry) {
                        if (first) {
                            first = false;
                        } else {
                            sb.append(",");
                        }
                        sb.append(colVal);
                    }
                    logger.debug("Array of column values[{}]: [" + sb.toString() + "]", outerIdx);
                    outerIdx++;
                }
            }

            Serie[] series = {serie};

            this.influxDB.write(this.configuration.getInfluxDBConfiguration().getName(), series, TimeUnit.SECONDS);

            context.stop();
            long endTime = System.currentTimeMillis();
            logger.debug("Commiting batch took " + (endTime - startTime) / 1000 + " seconds");

        } catch (Exception e) {
            logger.error("Failed to write alarm state history to database", e);
        }

        this.alarmStateTransitionedEventList.clear();
    }
}
