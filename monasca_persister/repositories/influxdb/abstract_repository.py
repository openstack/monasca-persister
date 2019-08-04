# (C) Copyright 2016-2017 Hewlett Packard Enterprise Development LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import abc
import influxdb
from oslo_config import cfg
import six

from monasca_persister.repositories import abstract_repository

DATABASE_NOT_FOUND_MSG = "database not found"


@six.add_metaclass(abc.ABCMeta)
class AbstractInfluxdbRepository(abstract_repository.AbstractRepository):

    def __init__(self):
        super(AbstractInfluxdbRepository, self).__init__()
        self.conf = cfg.CONF
        self._influxdb_client = influxdb.InfluxDBClient(
            self.conf.influxdb.ip_address,
            self.conf.influxdb.port,
            self.conf.influxdb.user,
            self.conf.influxdb.password)

    def write_batch(self, data_points_by_tenant):
        if self.conf.influxdb.db_per_tenant:
            for tenant_id, data_points in data_points_by_tenant.items():
                database = '%s_%s' % (self.conf.influxdb.database_name, tenant_id)
                self._write_batch(data_points, database)
        else:
            # NOTE (brtknr): Chain list of values to avoid multiple calls to
            # database API endpoint (when db_per_tenant is False).
            data_points = data_points_by_tenant.chained()
            self._write_batch(data_points, self.conf.influxdb.database_name)

    def _write_batch(self, data_points, database):
        # NOTE (brtknr): Loop twice to ensure database is created if missing.
        for retry in range(2):
            try:
                self._influxdb_client.write_points(data_points, 'ms',
                                                   protocol='line',
                                                   database=database)
                break
            except influxdb.exceptions.InfluxDBClientError as ex:
                if (str(ex).startswith(DATABASE_NOT_FOUND_MSG) and
                        self.conf.influxdb.db_per_tenant):
                    self._influxdb_client.create_database(database)
                else:
                    raise
