package com.hpcloud.mon.persister.repository;

import com.hpcloud.mon.common.event.AlarmStateTransitionedEvent;

public interface AlarmRepository {

    public void addToBatch(AlarmStateTransitionedEvent message);

    public void flush();
}
