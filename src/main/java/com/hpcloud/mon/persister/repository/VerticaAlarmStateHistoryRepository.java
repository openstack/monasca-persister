package com.hpcloud.mon.persister.repository;

import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.message.AlarmStateTransitionMessage;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class VerticaAlarmStateHistoryRepository extends VerticaRepository {

    private static final Logger logger = LoggerFactory.getLogger(VerticaAlarmStateHistoryRepository.class);
    private final MonPersisterConfiguration configuration;
    private static final String SQL_INSERT_INTO_ALARM_HISTORY =
            "insert into MonAlarms.StateHistory (tenant_id, alarm_id, alarm_name, old_state, new_state, reason, time_stamp) values (:tenant_id, :alarm_id, :alarm_name, :old_state, :new_state, :reason, :time_stamp)";
    private PreparedBatch batch;
    private final Timer commitTimer = Metrics.newTimer(this.getClass(), "commits-timer");
    private final SimpleDateFormat simpleDateFormat;

    @Inject
    public VerticaAlarmStateHistoryRepository(DBI dbi, MonPersisterConfiguration configuration) throws NoSuchAlgorithmException, SQLException {
        super(dbi);
        logger.debug("Instantiating: " + this);

        this.configuration = configuration;

        handle.getConnection().setAutoCommit(false);
        batch = handle.prepareBatch(SQL_INSERT_INTO_ALARM_HISTORY);
        handle.begin();

        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT-0"));
    }

    public void addToBatch(AlarmStateTransitionMessage message) {
        String timeStamp = simpleDateFormat.format(new Date(message.timestamp * 1000));
        batch.add().bind(0, message.tenantId).bind(1, message.alarmId).bind(2, message.alarmName).bind(3, message.oldState).bind(4, message.newState).bind(5, message.stateChangeReason).bind(6, timeStamp);
    }

    public void flush() {
        commitBatch();
    }

    private void commitBatch() {
        long startTime = System.currentTimeMillis();
        TimerContext context = commitTimer.time();
        batch.execute();
        handle.commit();
        handle.begin();
        context.stop();
        long endTime = System.currentTimeMillis();
        logger.debug("Commiting batch took " + (endTime - startTime) / 1000 + " seconds");
    }
}