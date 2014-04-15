package com.hpcloud.mon.persister.disruptor.event;

import com.lmax.disruptor.EventFactory;

public class MetricFactory implements EventFactory<MetricHolder> {

    public static final MetricFactory INSTANCE = new MetricFactory();

    @Override
    public MetricHolder newInstance() {
        return new MetricHolder();
    }
}
