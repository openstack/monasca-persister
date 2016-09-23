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
import hashlib
import json

from cassandra import query
from oslo_log import log
import urllib

from monasca_persister.repositories.cassandra import abstract_repository
from monasca_persister.repositories.utils import parse_measurement_message

LOG = log.getLogger(__name__)


class MetricCassandraRepository(
    abstract_repository.AbstractCassandraRepository):

    def __init__(self):

        super(MetricCassandraRepository, self).__init__()

        self._insert_measurement_stmt = self.cassandra_session.prepare(
                'insert into measurements (tenant_id,'
                'region, metric_hash, time_stamp, value,'
                'value_meta) values (?, ?, ?, ?, ?, ?)')

        self._insert_metric_map_stmt = self.cassandra_session.prepare(
                'insert into metric_map (tenant_id,'
                'region, metric_hash, '
                'metric_map) values'
                '(?,?,?,?)')

    def process_message(self, message):

        (dimensions, metric_name, region, tenant_id, time_stamp, value,
         value_meta) = parse_measurement_message(message)

        metric_hash, metric_map = create_metric_hash(metric_name,
                                                     dimensions)

        measurement = (tenant_id.encode('utf8'),
                       region.encode('utf8'),
                       metric_hash,
                       time_stamp,
                       value,
                       json.dumps(value_meta, ensure_ascii=False).encode(
                           'utf8'))

        LOG.debug(measurement)

        return MetricMeasurementInfo(
                tenant_id.encode('utf8'),
                region.encode('utf8'),
                metric_hash,
                metric_map,
                measurement)

    def write_batch(self, metric_measurement_infos):

        for metric_measurement_info in metric_measurement_infos:

            self._batch_stmt.add(self._insert_measurement_stmt,
                                 metric_measurement_info.measurement)

            metric_map = (metric_measurement_info.tenant_id,
                          metric_measurement_info.region,
                          metric_measurement_info.metric_hash,
                          metric_measurement_info.metric_map)

            self._batch_stmt.add(self._insert_metric_map_stmt,
                                 metric_map)

        self.cassandra_session.execute(self._batch_stmt)

        self._batch_stmt = query.BatchStatement()


class MetricMeasurementInfo(object):

    def __init__(self, tenant_id, region, metric_hash, metric_map,
                 measurement):

        self.tenant_id = tenant_id
        self.region = region
        self.metric_hash = metric_hash
        self.metric_map = metric_map
        self.measurement = measurement


def create_metric_hash(metric_name, dimensions):

    dimensions['__name__'] = urllib.quote_plus(metric_name)

    hash_string = ''

    for dim_name in sorted(dimensions.iterkeys()):
        dimension = (urllib.quote_plus(dim_name) + '=' + urllib.quote_plus(
                dimensions[dim_name]))
        hash_string += dimension

    sha1_hash = hashlib.sha1(hash_string).hexdigest()

    return bytearray.fromhex(sha1_hash), dimensions
