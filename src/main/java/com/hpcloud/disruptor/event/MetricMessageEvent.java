package com.hpcloud.disruptor.event;

import com.hpcloud.message.MetricEnvelope;

public class MetricMessageEvent {

    public MetricEnvelope getMetricEnvelope() {
        return metricEnvelope;
    }

    public void setMetricEnvelope(MetricEnvelope metricEnvelope) {
        this.metricEnvelope = metricEnvelope;
    }

    public MetricEnvelope metricEnvelope;

}
