package com.hpcloud.mon.persister.healthcheck;

import com.codahale.metrics.health.HealthCheck;

public class SimpleHealthCheck extends HealthCheck {

    public SimpleHealthCheck() {

    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }
}
