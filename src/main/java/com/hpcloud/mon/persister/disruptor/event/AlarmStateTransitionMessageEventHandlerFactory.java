package com.hpcloud.mon.persister.disruptor.event;

import com.google.inject.assistedinject.Assisted;

public interface AlarmStateTransitionMessageEventHandlerFactory {
    AlarmStateTransitionMessageEventHandler create(@Assisted("ordinal") int ordinal,
                                                   @Assisted("numProcessors") int numProcessors,
                                                   @Assisted("batchSize") int batchSize);
}
