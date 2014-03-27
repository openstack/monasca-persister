package com.hpcloud.mon.persister;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.consumer.AlarmStateTransitionsConsumer;
import com.hpcloud.mon.persister.consumer.MetricsConsumer;
import com.hpcloud.mon.persister.repository.RepositoryCommitHeartbeat;
import com.hpcloud.mon.persister.healthcheck.SimpleHealthCheck;
import com.hpcloud.mon.persister.resource.Resource;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

import javax.inject.Inject;

public class MonPersisterService extends Service<MonPersisterConfiguration> {

    public static void main(String[] args) throws Exception {
        new MonPersisterService().run(args);
    }

    @Override
    public void initialize(Bootstrap<MonPersisterConfiguration> bootstrap) {
        bootstrap.setName("mon-persister");
    }

    @Inject private kafka.javaapi.consumer.ConsumerConnector consumerConnector;

    @Override
    public void run(MonPersisterConfiguration configuration, Environment environment) throws Exception {

        Injector injector = Guice.createInjector(new MonPersisterModule(configuration, environment));

        // Sample resource.
        environment.addResource(new Resource());

        // Sample health check.
        environment.addHealthCheck(new SimpleHealthCheck("test-health-check"));

        MetricsConsumer metricsConsumer = injector.getInstance(MetricsConsumer.class);
        environment.manage(metricsConsumer);

        AlarmStateTransitionsConsumer alarmStateTransitionsConsumer = injector.getInstance(AlarmStateTransitionsConsumer.class);
        environment.manage(alarmStateTransitionsConsumer);

        RepositoryCommitHeartbeat repositoryCommitHeartbeat = injector.getInstance(RepositoryCommitHeartbeat.class);
        environment.manage(repositoryCommitHeartbeat);
    }
}