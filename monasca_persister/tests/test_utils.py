# (C) Copyright 2019 Fujitsu Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import mock

from oslotest import base
import simplejson as json

from monasca_persister.repositories import utils


class TestUtils(base.BaseTestCase):
    def setUp(self):
        super(TestUtils, self).setUp()

    def tearDown(self):
        super(TestUtils, self).tearDown()

    def test_parse_measurement_message(self):
        message = mock.Mock()
        message.value.return_value = """{
            "metric": {
              "name": "metric_name",
              "timestamp": "metric_timestamp",
              "value": "0.0",
              "value_meta": {

               },
              "dimensions": {}
          },
          "meta": {
              "region": "meta_region",
              "tenantId": "meta_tenantId"
          }
        }"""
        data = utils.parse_measurement_message(message)

        self.assertEqual(data[0], {})
        self.assertEqual(data[1], 'metric_name')
        self.assertEqual(data[2], 'meta_region')
        self.assertEqual(data[3], 'meta_tenantId')
        self.assertEqual(data[4], 'metric_timestamp')
        self.assertEqual(data[5], 0.0)
        self.assertEqual(data[6], {})

    def test_parse_alarm_state_hist_message(self):
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
        output = {'sub_alarm_expression': 'dummy_sub_alarm',
                  'current_values': 'dummy_values',
                  'metric_definition': 'dummy_definition',
                  'sub_alarm_state': 'dummy_state'}
        data = utils.parse_alarm_state_hist_message(message)
        self.assertEqual(data[0], 'dummyid')
        self.assertEqual(data[1], 'dummymetrics')
        self.assertEqual(data[2], 'dummynewState')
        self.assertEqual(data[3], 'dummyoldState')
        self.assertEqual(data[4], 'dummylink')
        self.assertEqual(data[5], 'dummylifecycleState')
        self.assertEqual(data[6], 'dummystateChangeReason')
        sub_alarms_data = json.loads(data[7])
        for elemKey, elemValue in output.items():
            self.assertIn(elemValue, sub_alarms_data[elemKey])
        self.assertEqual(data[8], 'dummytenantId')
        self.assertEqual(data[9], 'dummytimestamp')

    def test_parse_events_message(self):
        message = mock.Mock()
        message.value.return_value = """{
            "event": {
                "event_type": "dummy_event_type",
                "timestamp": "dummy_timestamp",
                "payload": "dummy_payload",
                "dimensions": "dummy_dimensions"
            },
            "meta": {
                "project_id": "dummy_project_id"
            }
        }"""

        (project_id, timestamp, event_type, payload,
         dimensions), _ = utils.parse_events_message(message)

        self.assertEqual(project_id, "dummy_project_id")
        self.assertEqual(timestamp, "dummy_timestamp")
        self.assertEqual(event_type, "dummy_event_type")
        self.assertEqual(payload, "dummy_payload")
        self.assertEqual(dimensions, "dummy_dimensions")
