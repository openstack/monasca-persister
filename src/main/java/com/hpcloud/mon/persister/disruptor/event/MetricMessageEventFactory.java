package com.hpcloud.mon.persister.disruptor.event;

import com.lmax.disruptor.EventFactory;

public class MetricMessageEventFactory implements EventFactory<MetricMessageEvent> {

    public static final MetricMessageEventFactory INSTANCE = new MetricMessageEventFactory();

    @Override
    public MetricMessageEvent newInstance() {
        return new MetricMessageEvent();
    }
}
