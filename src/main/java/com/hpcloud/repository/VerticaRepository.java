package com.hpcloud.repository;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VerticaRepository {
    private static final Logger logger = LoggerFactory.getLogger(VerticaRepository.class);
    protected DBI dbi;
    protected Handle handle;

    public VerticaRepository(DBI dbi) {
        this.dbi = dbi;
        this.handle = dbi.open();
        this.handle.execute("SET TIME ZONE TO 'UTC'");
    }

    public VerticaRepository() {
    }

    public void setDBI(DBI dbi)
            throws Exception {
        this.dbi = dbi;
        this.handle = dbi.open();
    }
}
