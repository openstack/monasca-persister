# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
# (C) Copyright 2017 SUSE LLC
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
from cachetools import LRUCache
from collections import namedtuple
import hashlib
import threading

from cassandra.concurrent import execute_concurrent
from oslo_log import log
import simplejson as json

from monasca_persister.repositories.cassandra import abstract_repository
from monasca_persister.repositories.cassandra import token_range_query_manager
from monasca_persister.repositories.cassandra.metric_batch import MetricBatch
from monasca_persister.repositories.utils import parse_measurement_message

LOG = log.getLogger(__name__)

MEASUREMENT_INSERT_CQL = (
    'update monasca.measurements USING TTL ? '
    'set value = ?, value_meta = ?, region = ?, tenant_id = ?, metric_name = ?, dimensions = ? '
    'where metric_id = ? and time_stamp = ?')

MEASUREMENT_UPDATE_CQL = ('update monasca.measurements USING TTL ? '
                          'set value = ?, value_meta = ? where metric_id = ? and time_stamp = ?')

METRICS_INSERT_CQL = ('update monasca.metrics USING TTL ? '
                      'set metric_id = ?, created_at = ?, updated_at = ? '
                      'where region = ? and tenant_id = ? and metric_name = ? and dimensions = ? '
                      'and dimension_names = ?')

METRICS_UPDATE_CQL = ('update monasca.metrics USING TTL ? '
                      'set metric_id = ?, updated_at = ? '
                      'where region = ? and tenant_id = ? and metric_name = ? and dimensions = ? '
                      'and dimension_names = ?')

DIMENSION_INSERT_CQL = ('insert into  monasca.dimensions '
                        '(region, tenant_id, name, value) values (?, ?, ?, ?)')

DIMENSION_METRIC_INSERT_CQL = ('insert into monasca.dimensions_metrics '
                               '(region, tenant_id, dimension_name, dimension_value, metric_name) '
                               'values (?, ?, ?, ?, ?)')

METRIC_DIMENSION_INSERT_CQL = ('insert into monasca.metrics_dimensions '
                               '(region, tenant_id, metric_name, dimension_name, dimension_value) '
                               'values (?, ?, ?, ?, ?)')

RETRIEVE_DIMENSION_CQL = 'select region, tenant_id, name, value from dimensions'

RETRIEVE_METRIC_DIMENSION_CQL = ('select region, tenant_id, metric_name, '
                                 'dimension_name, dimension_value from metrics_dimensions '
                                 'WHERE token(region, tenant_id, metric_name) > ? '
                                 'and token(region, tenant_id, metric_name) <= ? ')

Metric = namedtuple('Metric',
                    ['id',
                     'region',
                     'tenant_id',
                     'name',
                     'dimension_list',
                     'dimension_names',
                     'time_stamp',
                     'value',
                     'value_meta'])


class MetricCassandraRepository(abstract_repository.AbstractCassandraRepository):
    def __init__(self):
        super(MetricCassandraRepository, self).__init__()

        self._lock = threading.RLock()

        LOG.debug("prepare cql statements...")

        self._measurement_insert_stmt = self._session.prepare(MEASUREMENT_INSERT_CQL)
        self._measurement_insert_stmt.is_idempotent = True

        self._measurement_update_stmt = self._session.prepare(MEASUREMENT_UPDATE_CQL)
        self._measurement_update_stmt.is_idempotent = True

        self._metric_insert_stmt = self._session.prepare(METRICS_INSERT_CQL)
        self._metric_insert_stmt.is_idempotent = True

        self._metric_update_stmt = self._session.prepare(METRICS_UPDATE_CQL)
        self._metric_update_stmt.is_idempotent = True

        self._dimension_stmt = self._session.prepare(DIMENSION_INSERT_CQL)
        self._dimension_stmt.is_idempotent = True

        self._dimension_metric_stmt = self._session.prepare(DIMENSION_METRIC_INSERT_CQL)
        self._dimension_metric_stmt.is_idempotent = True

        self._metric_dimension_stmt = self._session.prepare(METRIC_DIMENSION_INSERT_CQL)
        self._metric_dimension_stmt.is_idempotent = True

        self._retrieve_metric_dimension_stmt = self._session.prepare(RETRIEVE_METRIC_DIMENSION_CQL)

        self._metric_batch = MetricBatch(
            self._cluster.metadata,
            self._cluster.load_balancing_policy,
            self._max_batches)

        self._metric_id_cache = LRUCache(self._cache_size)
        self._dimension_cache = LRUCache(self._cache_size)
        self._metric_dimension_cache = LRUCache(self._cache_size)

        self._load_dimension_cache()
        self._load_metric_dimension_cache()

    def process_message(self, message):
        (dimensions, metric_name, region, tenant_id, time_stamp, value,
         value_meta) = parse_measurement_message(message)

        with self._lock:
            dim_names = []
            dim_list = []
            for name in sorted(dimensions.iterkeys()):
                dim_list.append('%s\t%s' % (name, dimensions[name]))
                dim_names.append(name)

            hash_string = '%s\0%s\0%s\0%s' % (region, tenant_id, metric_name, '\0'.join(dim_list))
            metric_id = hashlib.sha1(hash_string.encode('utf8')).hexdigest()

            # TODO(brtknr): If database per tenant becomes the default and the
            # only option, recording tenant_id will be redundant.
            metric = Metric(id=metric_id,
                            region=region,
                            tenant_id=tenant_id,
                            name=metric_name,
                            dimension_list=dim_list,
                            dimension_names=dim_names,
                            time_stamp=time_stamp,
                            value=value,
                            value_meta=json.dumps(value_meta, ensure_ascii=False))

            id_bytes = bytearray.fromhex(metric.id)
            if self._metric_id_cache.get(metric.id, None):
                measurement_bound_stmt = self._measurement_update_stmt.bind((self._retention,
                                                                             metric.value,
                                                                             metric.value_meta,
                                                                             id_bytes,
                                                                             metric.time_stamp))
                self._metric_batch.add_measurement_query(measurement_bound_stmt)

                metric_update_bound_stmt = self._metric_update_stmt.bind((self._retention,
                                                                          id_bytes,
                                                                          metric.time_stamp,
                                                                          metric.region,
                                                                          metric.tenant_id,
                                                                          metric.name,
                                                                          metric.dimension_list,
                                                                          metric.dimension_names))
                self._metric_batch.add_metric_query(metric_update_bound_stmt)

                return metric, tenant_id

            self._metric_id_cache[metric.id] = metric.id

            metric_insert_bound_stmt = self._metric_insert_stmt.bind((self._retention,
                                                                      id_bytes,
                                                                      metric.time_stamp,
                                                                      metric.time_stamp,
                                                                      metric.region,
                                                                      metric.tenant_id,
                                                                      metric.name,
                                                                      metric.dimension_list,
                                                                      metric.dimension_names))
            self._metric_batch.add_metric_query(metric_insert_bound_stmt)

            for dim in metric.dimension_list:
                (name, value) = dim.split('\t')
                dim_key = self._get_dimnesion_key(metric.region, metric.tenant_id, name, value)
                if not self._dimension_cache.get(dim_key, None):
                    dimension_bound_stmt = self._dimension_stmt.bind((metric.region,
                                                                      metric.tenant_id,
                                                                      name,
                                                                      value))
                    self._metric_batch.add_dimension_query(dimension_bound_stmt)
                    self._dimension_cache[dim_key] = dim_key

                metric_dim_key = self._get_metric_dimnesion_key(
                    metric.region, metric.tenant_id, metric.name, name, value)
                if not self._metric_dimension_cache.get(metric_dim_key, None):
                    dimension_metric_bound_stmt = self._dimension_metric_stmt.bind(
                        (metric.region, metric.tenant_id, name, value, metric.name))
                    self._metric_batch.add_dimension_metric_query(dimension_metric_bound_stmt)

                    metric_dimension_bound_stmt = self._metric_dimension_stmt.bind(
                        (metric.region, metric.tenant_id, metric.name, name, value))
                    self._metric_batch.add_metric_dimension_query(metric_dimension_bound_stmt)

                    self._metric_dimension_cache[metric_dim_key] = metric_dim_key

            measurement_insert_bound_stmt = self._measurement_insert_stmt.bind(
                (self._retention,
                 metric.value,
                 metric.value_meta,
                 metric.region,
                 metric.tenant_id,
                 metric.name,
                 metric.dimension_list,
                 id_bytes,
                 metric.time_stamp))
            self._metric_batch.add_measurement_query(measurement_insert_bound_stmt)

            return metric, tenant_id

    def write_batch(self, metrics):
        with self._lock:
            batch_list = self._metric_batch.get_all_batches()
            results = execute_concurrent(self._session, batch_list, raise_on_first_error=True)
            self._handle_results(results)
            self._metric_batch.clear()
            LOG.info("flushed %s metrics", len(metrics))

    @staticmethod
    def _handle_results(results):
        for (success, result) in results:
            if not success:
                raise result

    def _load_dimension_cache(self):

        rows = self._session.execute(RETRIEVE_DIMENSION_CQL)

        if not rows:
            return

        for row in rows:
            key = self._get_dimnesion_key(row.region, row.tenant_id, row.name, row.value)
            self._dimension_cache[key] = key

        LOG.info(
            "loaded %s dimension entries cache from database into cache." %
            self._dimension_cache.currsize)

    @staticmethod
    def _get_dimnesion_key(region, tenant_id, name, value):
        return '%s\0%s\0%s\0%s' % (region, tenant_id, name, value)

    def _load_metric_dimension_cache(self):
        qm = token_range_query_manager.TokenRangeQueryManager(RETRIEVE_METRIC_DIMENSION_CQL,
                                                              self._process_metric_dimension_query)

        token_ring = self._cluster.metadata.token_map.ring

        qm.query(token_ring)

    def _process_metric_dimension_query(self, rows):

        cnt = 0
        for row in rows:
            key = self._get_metric_dimnesion_key(
                row.region,
                row.tenant_id,
                row.metric_name,
                row.dimension_name,
                row.dimension_value)
            self._metric_dimension_cache[key] = key
            cnt += 1

        LOG.info("loaded %s metric dimension entries from database into cache." % cnt)
        LOG.info(
            "total loaded %s metric dimension entries in cache." %
            self._metric_dimension_cache.currsize)

    @staticmethod
    def _get_metric_dimnesion_key(region, tenant_id, metric_name, dimension_name, dimension_value):

        return '%s\0%s\0%s\0%s\0%s' % (region, tenant_id, metric_name,
                                       dimension_name, dimension_value)
