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


@six.add_metaclass(abc.ABCMeta)
class AbstractInfluxdbRepository(abstract_repository.AbstractRepository):

    def __init__(self):
        super(AbstractInfluxdbRepository, self).__init__()
        self.conf = cfg.CONF
        self._influxdb_client = influxdb.InfluxDBClient(
            self.conf.influxdb.ip_address,
            self.conf.influxdb.port,
            self.conf.influxdb.user,
            self.conf.influxdb.password,
            self.conf.influxdb.database_name)

    def write_batch(self, data_points):
        self._influxdb_client.write_points(data_points, 'ms', protocol='line')
