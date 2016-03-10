# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
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
from influxdb import InfluxDBClient
from oslo_config import cfg
import six

from repositories.abstract_repository import AbstractRepository


@six.add_metaclass(abc.ABCMeta)
class AbstractInfluxdbRepository(AbstractRepository):

    def __init__(self):
        super(AbstractInfluxdbRepository, self).__init__()
        self.conf = cfg.CONF
        self._influxdb_client = InfluxDBClient(self.conf.influxdb.ip_address,
                                               self.conf.influxdb.port,
                                               self.conf.influxdb.user,
                                               self.conf.influxdb.password,
                                               self.conf.influxdb.database_name)

    def write_batch(self, data_points):
        self._influxdb_client.write_points(data_points, 'ms')
