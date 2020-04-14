# (C) Copyright 2019 Fujitsu Limited
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

from unittest import mock

from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement
from oslotest import base

from monasca_persister.repositories.cassandra import metric_batch


class FakeException(Exception):
    pass


class TestMetricBatch(base.BaseTestCase):
    def setUp(self):
        super(TestMetricBatch, self).setUp()
        self.bound_stmt = SimpleStatement("whatever")
        self.metric_batch = metric_batch.MetricBatch(mock.Mock(),
                                                     mock.Mock(), 1)
        self._set_patchers()
        self._set_mocks()

    def _set_patchers(self):
        self.patch_from_key = mock.patch.object(
            self.metric_batch.metadata.token_map.token_class, 'from_key')
        self.patch_lb_make_query_plan = mock.patch.object(
            self.metric_batch.lb_policy, 'make_query_plan')

    def _set_mocks(self):
        self.mock_key = self.patch_from_key.start()
        self.mock_lb_make_query = self.patch_lb_make_query_plan.start()
        self.mock_key.return_value = 'token'
        self.mock_lb_make_query.return_value = 'token'

    def tearDown(self):
        super(TestMetricBatch, self).tearDown()
        self.mock_key.reset_mock()
        self.patch_from_key.stop()
        self.mock_lb_make_query.reset_mock()
        self.patch_lb_make_query_plan.stop()

    def test_batch_query_by_token_creates_a_query_map_tuple(self):
        query_map = {}

        self.metric_batch.batch_query_by_token(self.bound_stmt, query_map)
        batch_statement, counter = query_map.get('token')[0]

        self.assertIsInstance(batch_statement, BatchStatement)
        self.assertIsInstance(counter, metric_batch.Counter)

    def test_batch_query_increments_counter_if_counter_value_lt_batch_limit(self):
        self.metric_batch.batch_limit = 2
        query_map = {}

        self.metric_batch.batch_query_by_token(self.bound_stmt, query_map)
        prev_counter = query_map.get('token')[0][1].value()

        self.metric_batch.batch_query_by_token(self.bound_stmt, query_map)
        current_counter = query_map.get('token')[0][1].value()

        self.assertEqual(prev_counter + 1, current_counter)

    def test_new_batch_added_if_counter_value_gt_or_eq_batch_limit(self):
        self.metric_batch.batch_limit = 1
        query_map = {}

        self.metric_batch.batch_query_by_token(self.bound_stmt, query_map)
        prev_len = len(query_map.get('token'))

        self.metric_batch.batch_query_by_token(self.bound_stmt, query_map)
        curr_len = len(query_map.get('token'))

        self.assertEqual(prev_len + 1, curr_len)

    def test_batch_query_by_replicas_creates_a_query_map_tuple(self):
        query_map = {}

        self.metric_batch.batch_query_by_replicas(self.bound_stmt, query_map)
        batch_statement, counter = query_map.get(tuple('token'))[0]

        self.assertIsInstance(batch_statement, BatchStatement)
        self.assertIsInstance(counter, metric_batch.Counter)

    def test_batch_query_by_replicas_increments_counter_if_counter_value_lt_30(self):
        query_map = {}
        self.metric_batch.batch_query_by_replicas(self.bound_stmt, query_map)
        prev_counter = query_map.get(tuple('token'))[0][1]._count = 29

        self.metric_batch.batch_query_by_replicas(self.bound_stmt, query_map)
        current_counter = query_map.get(tuple('token'))[0][1].value()

        self.assertEqual(prev_counter + 1, current_counter)

    def test_batch_query_by_replicas_adds_new_batch_if_counter_value_gt_30(self):
        query_map = {}
        self.metric_batch.batch_query_by_replicas(self.bound_stmt, query_map)
        prev_len = len(query_map.get(tuple('token')))

        query_map.get(tuple('token'))[0][1]._count = 30
        self.metric_batch.batch_query_by_replicas(self.bound_stmt, query_map)
        curr_len = len(query_map.get(tuple('token')))

        self.assertEqual(prev_len + 1, curr_len)

    def test_add_metric_query(self):
        self.metric_batch.add_metric_query(self.bound_stmt)

    def test_add_dimension_query(self):
        self.metric_batch.add_dimension_query(self.bound_stmt)

    def test_add_dimension_metric_query(self):
        self.metric_batch.add_dimension_metric_query(self.bound_stmt)

    def test_add_metric_dimension_query(self):
        self.metric_batch.add_metric_dimension_query(self.bound_stmt)

    def test_add_measurement_query(self):
        self.metric_batch.add_measurement_query(self.bound_stmt)

    def test_clear(self):
        self.metric_batch.clear()

        self.assertEqual(self.metric_batch.metric_queries, {})
        self.assertEqual(self.metric_batch.dimension_queries, {})
        self.assertEqual(self.metric_batch.dimension_metric_queries, {})
        self.assertEqual(self.metric_batch.metric_dimension_queries, {})
        self.assertEqual(self.metric_batch.measurement_queries, {})

    def test_log_token_batch_map(self):
        query_map = {}
        self.metric_batch.batch_query_by_token(self.bound_stmt, query_map)
        with mock.patch.object(metric_batch.LOG, 'info') as mock_log_info:
            self.metric_batch.log_token_batch_map('name', query_map)
            mock_log_info.assert_called_with('name : Size: 1;  Tokens: |token: 1|')

    def test_log_replica_batch_map(self):
        query_map = {}
        with mock.patch.object(self.metric_batch.lb_policy, 'make_query_plan'):
            with mock.patch.object(metric_batch.LOG, 'info') as mock_log_info:
                self.metric_batch.batch_query_by_replicas(self.bound_stmt, query_map)
                self.metric_batch.log_replica_batch_map('name', query_map)
                mock_log_info.assert_called_with('name : Size: 1;  Replicas: |: 1|')

    def test_get_all_batches(self):
        with mock.patch.object(self.metric_batch, 'log_token_batch_map'):
            with(mock.patch.object(self.metric_batch, 'log_replica_batch_map')):
                sample_elements = ['measurement_query', 'metric_query', 'dimension_query',
                                   'dimension_metric_query', 'metric_dimension_query']
                self.metric_batch.measurement_queries.update({'some': sample_elements[0]})
                self.metric_batch.metric_queries.update({'some': sample_elements[1]})
                self.metric_batch.dimension_queries.update({'some': sample_elements[2]})
                self.metric_batch.dimension_metric_queries.update({'some': sample_elements[3]})
                self.metric_batch.metric_dimension_queries.update({'some': sample_elements[4]})

                result_list = self.metric_batch.get_all_batches()

                for elem in sample_elements:
                    self.assertIn(elem[0], result_list)

    def test_counter_increment_by(self):
        counter = metric_batch.Counter()
        temp_counter_value = counter.value()
        counter.increment_by(5)

        self.assertEqual(temp_counter_value + 5, counter.value())
