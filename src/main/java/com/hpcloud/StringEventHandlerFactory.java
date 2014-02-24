package com.hpcloud;

import com.google.inject.assistedinject.Assisted;

public interface StringEventHandlerFactory {
    StringEventHandler create(@Assisted("ordinal") int ordinal,
                              @Assisted("numProcessors") int numProcessors,
                              @Assisted("batchSize") int batchSize);
}
