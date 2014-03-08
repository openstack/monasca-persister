package com.hpcloud;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.hpcloud.configuration.MonPersisterConfiguration;
import com.hpcloud.consumer.MonConsumer;
import com.hpcloud.dedupe.MonDeDuperHeartbeat;
import com.hpcloud.healthcheck.SimpleHealthCheck;
import com.hpcloud.resource.Resource;
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

        MonConsumer monConsumer = injector.getInstance(MonConsumer.class);
        environment.manage(monConsumer);

        MonDeDuperHeartbeat monDeDuperHeartbeat = injector.getInstance(MonDeDuperHeartbeat.class);
        environment.manage(monDeDuperHeartbeat);
    }

}
