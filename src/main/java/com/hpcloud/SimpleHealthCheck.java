package com.hpcloud;

import com.yammer.metrics.core.HealthCheck;

public class SimpleHealthCheck extends HealthCheck {

    protected SimpleHealthCheck(String name) {
        super(name);
    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }
}
