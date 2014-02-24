package com.hpcloud.healthcheck;

import com.yammer.metrics.core.HealthCheck;

public class SimpleHealthCheck extends HealthCheck {

    public SimpleHealthCheck(String name) {
        super(name);
    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }
}
