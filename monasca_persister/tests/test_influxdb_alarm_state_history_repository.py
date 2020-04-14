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

from monasca_persister.repositories.influxdb.alarm_state_history_repository \
    import AlarmStateHistInfluxdbRepository
from monasca_persister.repositories.influxdb import abstract_repository


class TestInfluxdbAlarmStateHistoryRepo(base.BaseTestCase):
    def setUp(self):
        super(TestInfluxdbAlarmStateHistoryRepo, self).setUp()
        with mock.patch.object(abstract_repository.cfg, 'CONF',
                               return_value=mock.Mock()):
            self.alarm_state_repo = AlarmStateHistInfluxdbRepository()

    def tearDown(self):
        super(TestInfluxdbAlarmStateHistoryRepo, self).tearDown()

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
                "timestamp": "10",
                "subAlarms": {
                    "subAlarmExpression": "dummy_sub_alarm",
                    "currentValues": "dummy_values",
                    "metricDefinition": "dummy_definition",
                    "subAlarmState": "dummy_state"
                }
            }
        }"""
        expected_output = 'alarm_state_history,tenant_id=dummytenantId ' \
                          'tenant_id="dummytenantId",alarm_id="dummyid",' \
                          'metrics="\\"dummymetrics\\"",new_state="dummynewState"' \
                          ',old_state="dummyoldState",link="dummylink",' \
                          'lifecycle_state="dummylifecycleState",' \
                          'reason="dummystateChangeReason",reason_data="{}"'
        expected_dict = ['\\"sub_alarm_expression\\": \\"dummy_sub_alarm\\"',
                         '\\"metric_definition\\": \\"dummy_definition\\"',
                         '\\"sub_alarm_state\\": \\"dummy_state\\"',
                         '\\"current_values\\": \\"dummy_values\\"']

        actual_output, tenant_id = self.alarm_state_repo.process_message(message)

        self.assertEqual(tenant_id, 'dummytenantId')
        self.assertIn(expected_output, actual_output)
        for elem in expected_dict:
            self.assertIn(elem, actual_output)
