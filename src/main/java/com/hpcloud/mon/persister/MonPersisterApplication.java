package com.hpcloud.mon.persister;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.consumer.AlarmStateTransitionsConsumer;
import com.hpcloud.mon.persister.consumer.MetricsConsumer;
import com.hpcloud.mon.persister.healthcheck.SimpleHealthCheck;
import com.hpcloud.mon.persister.repository.RepositoryCommitHeartbeat;
import com.hpcloud.mon.persister.resource.Resource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import javax.inject.Inject;

public class MonPersisterApplication extends Application<MonPersisterConfiguration> {

    public static void main(String[] args) throws Exception {
        new MonPersisterApplication().run(args);
    }

    @Override
    public void initialize(Bootstrap<MonPersisterConfiguration> bootstrap) {
    }

    @Override
    public String getName() {
        return "mon-persister";
    }

    @Inject private kafka.javaapi.consumer.ConsumerConnector consumerConnector;

    @Override
    public void run(MonPersisterConfiguration configuration, Environment environment) throws Exception {

        Injector injector = Guice.createInjector(new MonPersisterModule(configuration, environment));

        // Sample resource.
        environment.jersey().register(new Resource());

        // Sample health check.
        environment.healthChecks().register("test-health-check", new SimpleHealthCheck());

        MetricsConsumer metricsConsumer = injector.getInstance(MetricsConsumer.class);
        environment.lifecycle().manage(metricsConsumer);

        AlarmStateTransitionsConsumer alarmStateTransitionsConsumer = injector.getInstance(AlarmStateTransitionsConsumer.class);
        environment.lifecycle().manage(alarmStateTransitionsConsumer);

        RepositoryCommitHeartbeat repositoryCommitHeartbeat = injector.getInstance(RepositoryCommitHeartbeat.class);
        environment.lifecycle().manage(repositoryCommitHeartbeat);

    }
}