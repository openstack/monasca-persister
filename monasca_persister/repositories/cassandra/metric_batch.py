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

from cassandra.query import BatchStatement
from cassandra.query import BatchType
from oslo_log import log

LOG = log.getLogger(__name__)


class MetricBatch(object):
    def __init__(self, metadata, load_balance_policy, batch_limit):

        self.metadata = metadata
        self.batch_limit = batch_limit
        self.lb_policy = load_balance_policy
        self.metric_queries = dict()
        self.dimension_queries = dict()
        self.dimension_metric_queries = dict()
        self.metric_dimension_queries = dict()
        self.measurement_queries = dict()

    def batch_query_by_token(self, bound_stmt, query_map):
        token = self.metadata.token_map.token_class.from_key(bound_stmt.routing_key)

        queue = query_map.get(token, None)
        if not queue:
            queue = []
            batch = BatchStatement(BatchType.UNLOGGED)
            batch.add(bound_stmt)
            queue.append((batch, Counter(1)))
            query_map[token] = queue
        else:
            (batch, counter) = queue[-1]
            if counter.value() < self.batch_limit:
                batch.add(bound_stmt)
                counter.increment()
            else:
                batch = BatchStatement(BatchType.UNLOGGED)
                batch.add(bound_stmt)
                queue.append((batch, Counter(1)))

    def add_metric_query(self, bound_stmt):
        self.batch_query_by_token(bound_stmt, self.metric_queries)

    def add_dimension_query(self, bound_stmt):
        self.batch_query_by_token(bound_stmt, self.dimension_queries)

    def add_dimension_metric_query(self, bound_stmt):
        self.batch_query_by_token(bound_stmt, self.dimension_metric_queries)

    def add_metric_dimension_query(self, bound_stmt):
        self.batch_query_by_token(bound_stmt, self.metric_dimension_queries)

    def add_measurement_query(self, bound_stmt):
        self.batch_query_by_replicas(bound_stmt, self.measurement_queries)

    def batch_query_by_replicas(self, bound_stmt, query_map):
        hosts = tuple(
            self.lb_policy.make_query_plan(
                working_keyspace=bound_stmt.keyspace,
                query=bound_stmt))

        queue = query_map.get(hosts, None)
        if not queue:
            queue = []
            batch = BatchStatement(BatchType.UNLOGGED)
            batch.add(bound_stmt)
            queue.append((batch, Counter(1)))
            query_map[hosts] = queue
        else:
            (batch, counter) = queue[-1]
            if counter.value() < 30:
                batch.add(bound_stmt)
                counter.increment()
            else:
                batch = BatchStatement(BatchType.UNLOGGED)
                batch.add(bound_stmt)
                queue.append((batch, Counter(1)))

    def clear(self):
        self.metric_queries.clear()
        self.dimension_queries.clear()
        self.dimension_metric_queries.clear()
        self.metric_dimension_queries.clear()
        self.measurement_queries.clear()

    @staticmethod
    def log_token_batch_map(name, query_map):
        LOG.info('%s : Size: %s;  Tokens: |%s|' %
                 (name, len(query_map),
                  '|'.join(['%s: %s' % (
                      token,
                      ','.join([str(counter.value()) for (batch, counter) in queue]))
                      for token, queue in query_map.items()])))

    @staticmethod
    def log_replica_batch_map(name, query_map):
        LOG.info('%s : Size: %s;  Replicas: |%s|' %
                 (name, len(query_map), '|'.join([
                     '%s: %s' % (
                         ','.join([h.address for h in hosts]),
                         ','.join([str(counter.value()) for (batch, counter) in queue]))
                     for hosts, queue in query_map.items()])))

    def get_all_batches(self):
        self.log_token_batch_map("metric batches", self.metric_queries)
        self.log_token_batch_map("dimension batches", self.dimension_queries)
        self.log_token_batch_map("dimension metric batches", self.dimension_metric_queries)
        self.log_token_batch_map("metric dimension batches", self.metric_dimension_queries)
        self.log_replica_batch_map("measurement batches", self.measurement_queries)

        result_list = []

        for q in self.measurement_queries.values():
            result_list.extend(q)

        for q in self.metric_queries.values():
            result_list.extend(q)

        for q in self.dimension_queries.values():
            result_list.extend(q)

        for q in self.dimension_metric_queries.values():
            result_list.extend(q)

        for q in self.metric_dimension_queries.values():
            result_list.extend(q)

        return result_list


class Counter(object):
    def __init__(self, init_value=0):
        self._count = init_value

    def increment(self):
        self._count += 1

    def increment_by(self, increment):
        self._count += increment

    def value(self):
        return self._count
