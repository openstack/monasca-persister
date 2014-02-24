package com.hpcloud;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.ProvisionException;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import org.skife.jdbi.v2.DBI;

public class MonPersisterModule extends AbstractModule {

    private final MonPersisterConfiguration configuration;
    private final Environment environment;

    public MonPersisterModule(MonPersisterConfiguration configuration, Environment environment) {
        this.configuration = configuration;
        this.environment = environment;
    }

    @Override
    protected void configure() {

        bind(MonPersisterConfiguration.class).toInstance(configuration);

        install(new FactoryModuleBuilder()
                .implement(StringEventHandler.class, StringEventHandler.class)
                .build(StringEventHandlerFactory.class));

        bind(DisruptorFactory.class);

        bind(MonConsumer.class);

        bind(DBI.class).toProvider(new Provider<DBI>() {
            @Override
            public DBI get() {
                try {
                    return new DBIFactory().build(environment, configuration.getDatabaseConfiguration(), "vertica");
                } catch (ClassNotFoundException e) {
                    throw new ProvisionException("Failed to provision DBI", e);
                }
            }
        }).in(Scopes.SINGLETON);


    }
}
