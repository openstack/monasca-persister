package com.hpcloud.mon.persister.message;

import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName(value = "alarm-transitioned")
public class AlarmStateTransitionMessage {

    public String tenantId;
    public String alarmId;
    public String alarmName;
    public String oldState;
    public String newState;
    public String stateChangeReason;
    public long timestamp;

    private AlarmStateTransitionMessage() {
    }

    public AlarmStateTransitionMessage(String tenantId, String alarmId, String alarmName,
                                       String oldState, String newState, String stateChangeReason, long timestamp) {
        this.tenantId = tenantId;
        this.alarmId = alarmId;
        this.alarmName = alarmName;
        this.oldState = oldState;
        this.newState = newState;
        this.stateChangeReason = stateChangeReason;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format(
                "AlarmStateTransitionEvent [tenantId=%s, alarmId=%s, alarmName=%s, oldState=%s, newState=%s, stateChangeReason=%s, timestamp=%s]",
                tenantId, alarmId, alarmName, oldState, newState, stateChangeReason, timestamp);
    }
}
