package com.hpcloud.mon.persister.disruptor.event;

import com.hpcloud.mon.persister.message.MetricEnvelope;

public class MetricMessageEvent {

    public MetricEnvelope getMetricEnvelope() {
        return metricEnvelope;
    }

    public void setEnvelope(MetricEnvelope metricEnvelope) {
        this.metricEnvelope = metricEnvelope;
    }

    public MetricEnvelope metricEnvelope;

}
