package com.hpcloud.mon.persister.dbi;

import com.google.inject.ProvisionException;
import com.hpcloud.mon.persister.configuration.MonPersisterConfiguration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import org.skife.jdbi.v2.DBI;

import javax.inject.Inject;
import javax.inject.Provider;

public class DBIProvider implements Provider<DBI> {

    private final Environment environment;
    private final MonPersisterConfiguration configuration;

    @Inject
    public DBIProvider(Environment environment, MonPersisterConfiguration configuration) {
        this.environment = environment;
        this.configuration = configuration;
    }

    @Override
    public DBI get() {
        try {
            return new DBIFactory().build(environment, configuration.getDatabaseConfiguration(), "vertica");
        } catch (ClassNotFoundException e) {
            throw new ProvisionException("Failed to provision DBI", e);
        }
    }

}
