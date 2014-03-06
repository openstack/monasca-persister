package com.hpcloud.disruptor.event;

import com.hpcloud.message.MetricMessage;

public class MetricMessageEvent {

    public MetricMessage getMetricMessage() {
        return metricMessage;
    }

    public void setMetricMessage(MetricMessage metricMessage) {
        this.metricMessage = metricMessage;
    }

    public MetricMessage metricMessage;

}
