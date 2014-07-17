/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hpcloud.mon.persister;

import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.hpcloud.mon.persister.consumer.AlarmStateTransitionsConsumer;
import com.hpcloud.mon.persister.consumer.MetricsConsumer;
import com.hpcloud.mon.persister.healthcheck.SimpleHealthCheck;
import com.hpcloud.mon.persister.repository.RepositoryCommitHeartbeat;
import com.hpcloud.mon.persister.resource.Resource;

import com.google.inject.Guice;
import com.google.inject.Injector;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

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

  @Override
  public void run(MonPersisterConfiguration configuration, Environment environment)
      throws Exception {

    Injector injector = Guice.createInjector(new MonPersisterModule(configuration, environment));

    // Sample resource.
    environment.jersey().register(new Resource());

    // Sample health check.
    environment.healthChecks().register("test-health-check", new SimpleHealthCheck());

    MetricsConsumer metricsConsumer = injector.getInstance(MetricsConsumer.class);
    environment.lifecycle().manage(metricsConsumer);

    AlarmStateTransitionsConsumer alarmStateTransitionsConsumer =
        injector.getInstance(AlarmStateTransitionsConsumer.class);
    environment.lifecycle().manage(alarmStateTransitionsConsumer);

    RepositoryCommitHeartbeat repositoryCommitHeartbeat =
        injector.getInstance(RepositoryCommitHeartbeat.class);
    environment.lifecycle().manage(repositoryCommitHeartbeat);

  }
}
