package com.hpcloud.mon.persister;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.consumer.MonPersisterConsumer;
import com.hpcloud.mon.persister.dedupe.MonPersisterDeduperHeartbeat;
import com.hpcloud.mon.persister.healthcheck.SimpleHealthCheck;
import com.hpcloud.mon.persister.resource.Resource;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

public class MonPersisterService extends Service<MonPersisterConfiguration> {

    public static void main(String[] args) throws Exception {
        new MonPersisterService().run(args);
    }

    @Override
    public void initialize(Bootstrap<MonPersisterConfiguration> bootstrap) {
        bootstrap.setName("mon-persister");
    }

    @Override
    public void run(MonPersisterConfiguration configuration, Environment environment) throws Exception {

        Injector injector = Guice.createInjector(new MonPersisterModule(configuration, environment));

        // Sample resource.
        environment.addResource(new Resource());
        // Sample health check.
        environment.addHealthCheck(new SimpleHealthCheck("test-health-check"));

        MonPersisterConsumer consumer = injector.getInstance(MonPersisterConsumer.class);
        environment.manage(consumer);

        MonPersisterDeduperHeartbeat deduperHeartbeat = injector.getInstance(MonPersisterDeduperHeartbeat.class);
        environment.manage(deduperHeartbeat);
    }

}
