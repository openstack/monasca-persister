package com.hpcloud;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

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

        Disruptor<StringEvent> disruptor = createDisruptor(configuration);

        Injector injector = Guice.createInjector(new MonPersisterModule(configuration, disruptor));

        // Sample resource.
        environment.addResource(new Resource());
        // Sample health check.
        environment.addHealthCheck(new SimpleHealthCheck("test-health-check"));

        MonConsumer monConsumer = injector.getInstance(MonConsumer.class);
        environment.manage(monConsumer);
    }

    private Disruptor<StringEvent> createDisruptor(MonPersisterConfiguration configuration) {
        Executor executor = Executors.newCachedThreadPool();
        StringEventFactory stringEventFactory = new StringEventFactory();

        int buffersize = configuration.getDisruptorConfiguration().bufferSize;
        Disruptor<StringEvent> disruptor = new Disruptor(stringEventFactory, buffersize, executor);

        int numOutputProcessors = configuration.getVerticaOutputProcessorConfiguration().numProcessors;
        EventHandlerGroup<StringEvent> handlerGroup = null;
        for (int i = 0; i < numOutputProcessors; ++i) {

            StringEventHandler stringEventHandler = new StringEventHandler();

            if (handlerGroup == null) {
                handlerGroup = disruptor.handleEventsWith(stringEventHandler);
            } else {
                handlerGroup.then(stringEventHandler);
            }

        }
        disruptor.start();
        return disruptor;
    }
}
