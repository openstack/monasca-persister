package com.hpcloud;

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
       System.out.println("Hello, my name is " + configuration.getName());
        environment.addResource(new Resource());
    }
}
