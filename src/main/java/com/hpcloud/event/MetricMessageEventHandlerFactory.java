package com.hpcloud.event;

import com.google.inject.assistedinject.Assisted;

public interface MetricMessageEventHandlerFactory {
    MetricMessageEventHandler create(@Assisted("ordinal") int ordinal,
                                     @Assisted("numProcessors") int numProcessors,
                              @Assisted("batchSize") int batchSize);
}
