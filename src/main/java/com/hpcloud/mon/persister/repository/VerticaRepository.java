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
package com.hpcloud.mon.persister.repository;

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
