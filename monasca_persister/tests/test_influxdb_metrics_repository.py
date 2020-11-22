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

import influxdb
from influxdb.exceptions import InfluxDBClientError
from oslotest import base
from oslo_config import cfg

from monasca_persister.repositories.influxdb.metrics_repository import MetricInfluxdbRepository


db_not_found = InfluxDBClientError(
    content='{"error": "database not found: db"}', code=404)

class TestMetricInfluxdbRepository(base.BaseTestCase):

    def setUp(self):
        super(TestMetricInfluxdbRepository, self).setUp()

    def tearDown(self):
        super(TestMetricInfluxdbRepository, self).tearDown()

    def _test_process_message(self, metrics_repo, data_points, metric, tenant):
        _dp, _tenant = metrics_repo.process_message(metric)
        self.assertIsInstance(_dp, str)
        self.assertEqual(_tenant, tenant)
        data_points.append(_tenant, _dp)

    @mock.patch.object(influxdb, 'InfluxDBClient')
    @mock.patch.object(cfg, 'CONF', return_value=None)
    def _test_write_batch(self, mock_conf, mock_influxdb_client,
                          db_per_tenant, db_exists, hours=0):
        mock_conf.influxdb.database_name = db_name = 'db'
        mock_conf.influxdb.db_per_tenant = db_per_tenant
        mock_conf.influxdb.default_retention_hours = hours
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
            metrics_repo._influxdb_client.write_points = mock.Mock()
        else:
            metrics_repo._influxdb_client.write_points = mock.Mock(
                side_effect=[db_not_found, None, db_not_found, None])
            rp = '{}h'.format(hours)
            if db_per_tenant:
                db1 = '%s_%s' % (db_name, t1)
                db2 = '%s_%s' % (db_name, t2)
                rp1 = mock.call(database=db1, default=True, name=rp,
                                duration=rp, replication='1')
                rp2 = mock.call(database=db2, default=True, name=rp,
                                duration=rp, replication='1')
                calls = [mock.call(db1), mock.call(db2)]
                rp_calls = [rp1, rp2]
            else:
                calls = [mock.call(db_name)]
                rp_calls = [mock.call(database=db_name, default=True,
                                      name=rp, duration=rp, replication='1')]
        metrics_repo.write_batch(data_points)
        if db_exists:
            mock_influxdb_client.create_database.assert_not_called()
            mock_influxdb_client.create_retention_policy.assert_not_called()
        else:
            mock_influxdb_client.create_database.assert_has_calls(
                calls, any_order=True)
            if hours > 0:
                mock_influxdb_client.create_retention_policy.assert_has_calls(
                    rp_calls, any_order=True)
            else:
                mock_influxdb_client.create_retention_policy.assert_not_called()

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
        message = mock.Mock()
        message.value.return_value = metric
        return message

    def test_write_batch_db_exists(self):
        self._test_write_batch(db_per_tenant=False, db_exists=True)

    def test_write_batch(self):
        self._test_write_batch(db_per_tenant=False, db_exists=False)

    def test_write_batch_db_exists_with_rp(self):
        self._test_write_batch(db_per_tenant=False, db_exists=True, hours=2016)

    def test_write_batch_with_rp(self):
        self._test_write_batch(db_per_tenant=False, db_exists=False, hours=2016)

    def test_write_batch_db_per_tenant_db_exists(self):
        self._test_write_batch(db_per_tenant=True, db_exists=True)

    def test_write_batch_db_per_tenant(self):
        self._test_write_batch(db_per_tenant=True, db_exists=False)

    def test_write_batch_db_per_tenant_db_exists_with_rp(self):
        self._test_write_batch(db_per_tenant=True, db_exists=True, hours=2016)

    def test_write_batch_db_per_tenant_with_rp(self):
        self._test_write_batch(db_per_tenant=True, db_exists=False, hours=2016)
