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

from oslotest import base
from oslo_config import cfg

from monasca_persister.repositories.cassandra import alarm_state_history_repository
from monasca_persister.repositories.cassandra import connection_util
from monasca_persister.repositories import data_points


class TestAlarmStateHistoryRepo(base.BaseTestCase):
    def setUp(self):
        super(TestAlarmStateHistoryRepo, self).setUp()
        self._set_patchers()
        self._set_mocks()
        self.alarm_state_hist_repo = alarm_state_history_repository. \
            AlarmStateHistCassandraRepository()

    def tearDown(self):
        super(TestAlarmStateHistoryRepo, self).tearDown()

        self.mock_cfg.reset_mock()
        self.mock_conn_util_cluster.reset_mock()
        self.mock_conn_util_session.reset_mock()

        self.patch_cfg.stop()
        self.patch_conn_util_session.stop()
        self.patch_conn_util_cluster.stop()

    def _set_patchers(self):
        self.patch_cfg = mock.patch.object(
            alarm_state_history_repository.abstract_repository, 'conf')
        self.patch_conn_util_cluster = mock.patch.object(connection_util,
                                                         'create_cluster',
                                                         return_value=None)
        self.patch_conn_util_session =\
            mock.patch.object(connection_util, 'create_session',
                              return_value=mock.Mock(prepare=mock.Mock(
                                  return_value=None)))

    def _set_mocks(self):
        self.mock_cfg = self.patch_cfg.start()
        self.mock_conn_util_cluster = self.patch_conn_util_cluster.start()
        self.mock_conn_util_session = self.patch_conn_util_session.start()

    def test_process_message(self):
        message = mock.Mock()
        message.value.return_value = """{
            "alarm-transitioned": {
                "alarmId": "dummyid",
                "metrics": "dummymetrics",
                "newState": "dummynewState",
                "oldState": "dummyoldState",
                "link": "dummylink",
                "lifecycleState": "dummylifecycleState",
                "stateChangeReason": "dummystateChangeReason",
                "tenantId": "dummytenantId",
                "timestamp": "dummytimestamp",
                "subAlarms": {
                    "subAlarmExpression": "dummy_sub_alarm",
                    "currentValues": "dummy_values",
                    "metricDefinition": "dummy_definition",
                    "subAlarmState": "dummy_state"
                }
            }
        }"""
        self.alarm_state_hist_repo._retention = 0

        expected_output = [b'"sub_alarm_expression": "dummy_sub_alarm"',
                           b'"current_values": "dummy_values"',
                           b'"metric_definition": "dummy_definition"',
                           b'"sub_alarm_state": "dummy_state"']

        output, tenant_id = self.alarm_state_hist_repo.process_message(message)

        self.assertEqual(tenant_id, 'dummytenantId')
        self.assertEqual(output[0], self.alarm_state_hist_repo._retention)
        self.assertEqual(output[1], b'"dummymetrics"')
        self.assertEqual(output[2], b'dummyoldState')
        self.assertEqual(output[3], b'dummynewState')
        for elem in expected_output:
            self.assertIn(elem, output[4])
        self.assertEqual(output[5], b'dummystateChangeReason')
        self.assertEqual(output[6], b'{}')
        self.assertEqual(output[7], b'dummytenantId')
        self.assertEqual(output[8], b'dummyid')
        self.assertEqual(output[9], 'dummytimestamp')

    def test_write_batch(self):
        with mock.patch.object(alarm_state_history_repository,
                               'execute_concurrent_with_args',
                               return_value=0):
            cfg.CONF = mock.Mock(kafka_alarm_history=mock.Mock(batch_size=1))

            self._session, self._upsert_stmt = mock.Mock(), mock.Mock()
            alarm_state_hists_by_tenant = data_points.DataPointsAsList()
            alarm_state_hists_by_tenant.append('fake_tenant', 'elem')
            self.alarm_state_hist_repo.write_batch(alarm_state_hists_by_tenant)
