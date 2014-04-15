package com.hpcloud.mon.persister.disruptor.event;

import com.hpcloud.mon.common.model.metric.MetricEnvelope;

public class MetricHolder {

    MetricEnvelope metricEnvelope;

    public MetricEnvelope getMetricEnvelope() {
        return metricEnvelope;
    }

    public void setEnvelope(MetricEnvelope metricEnvelope) {
        this.metricEnvelope = metricEnvelope;
    }
}
