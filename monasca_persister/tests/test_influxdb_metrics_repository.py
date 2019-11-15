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

import influxdb
from influxdb.exceptions import InfluxDBClientError

from mock import Mock
from mock import patch
from mock import call

from monasca_persister.repositories.influxdb.metrics_repository import MetricInfluxdbRepository

from oslotest import base
from oslo_config import cfg

import six

db_not_found = InfluxDBClientError(
    content='{"error": "database not found: db"}', code=404)

class TestMetricInfluxdbRepository(base.BaseTestCase):

    def setUp(self):
        super(TestMetricInfluxdbRepository, self).setUp()

    def tearDown(self):
        super(TestMetricInfluxdbRepository, self).tearDown()

    def _test_process_message(self, metrics_repo, data_points, metric, tenant):
        _dp, _tenant = metrics_repo.process_message(metric)
        self.assertIsInstance(_dp, six.string_types)
        self.assertEqual(_tenant, tenant)
        data_points.append(_tenant, _dp)

    @patch.object(influxdb, 'InfluxDBClient')
    @patch.object(cfg, 'CONF', return_value=None)
    def _test_write_batch(self, mock_conf, mock_influxdb_client,
                          db_per_tenant, db_exists):
        mock_conf.influxdb.database_name = db_name = 'db'
        mock_conf.influxdb.db_per_tenant = db_per_tenant
        t1 = u'fake_tenant_id_1'
        t2 = u'fake_tenant_id_2'
        m1 = self._get_metric(t1)
        m2 = self._get_metric(t2)
        metrics_repo = MetricInfluxdbRepository()
        data_points = metrics_repo.data_points_class()
        self._test_process_message(metrics_repo, data_points, m1, t1)
        self._test_process_message(metrics_repo, data_points, m2, t2)
        metrics_repo._influxdb_client = mock_influxdb_client
        if db_exists:
            metrics_repo._influxdb_client.write_points = Mock()
        else:
            metrics_repo._influxdb_client.write_points = Mock(
                side_effect=[db_not_found, None, db_not_found, None])
            if db_per_tenant:
                call1 = call('%s_%s' % (db_name, t1))
                call2 = call('%s_%s' % (db_name, t2))
                calls = [call1, call2]
            else:
                calls = [call(db_name)]
        metrics_repo.write_batch(data_points)
        if db_exists:
            mock_influxdb_client.create_database.assert_not_called()
        else:
            mock_influxdb_client.create_database.assert_has_calls(
                calls, any_order=True)

    def _get_metric(self, tenant_id):
        metric = '''
            {
              "metric":
              {
                "timestamp":1554725987408.1049804688,
                "name":"process.cpu_perc",
                "dimensions":
                {
                  "process_name":"persister",
                  "hostname":"devstack",
                  "service":"persister"
                },
                "value":7.5,
                "value_meta":null},
                "meta":
                {
                  "region":"RegionOne",
                  "tenantId":"''' + tenant_id + '''"
                },
                "creation_time":1554725988
            }
        '''
        message = Mock()
        message.value.return_value = metric
        return message

    def test_write_batch_db_exists(self):
        self._test_write_batch(db_per_tenant=False, db_exists=True)

    def test_write_batch(self):
        self._test_write_batch(db_per_tenant=False, db_exists=False)

    def test_write_batch_db_per_tenant_db_exists(self):
        self._test_write_batch(db_per_tenant=True, db_exists=True)

    def test_write_batch_db_per_tenant(self):
        self._test_write_batch(db_per_tenant=True, db_exists=False)
