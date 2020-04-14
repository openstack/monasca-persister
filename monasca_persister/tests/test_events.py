# -*- coding: utf-8 -*-
# Copyright 2017 FUJITSU LIMITED
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
from datetime import datetime
import json
import os
from unittest import mock

from oslotest import base
from testtools import matchers

from monasca_persister.repositories.elasticsearch import events_repository
from monasca_persister.repositories import utils


class TestEvents(base.BaseTestCase):

    def __init__(self, *args, **kwds):
        super(TestEvents, self).__init__(*args, **kwds)
        self.events = None

    def setUp(self):
        super(TestEvents, self).setUp()

    def tearDown(self):
        super(TestEvents, self).tearDown()

    def test_parse_event(self):
        event = self._load_event('event_1')
        (project_id, timestamp, event_type, payload,
         dimensions), _ = utils.parse_events_message(event)
        self.assertEqual('de98fbff448f4f278a56e9929db70b03', project_id)
        self.assertEqual('2017-06-01 09:15:11.494606', timestamp)
        self.assertEqual('compute.instance.create.start', event_type)
        self.assertEqual('compute', dimensions['service'])
        self.assertEqual('notification.sample', dimensions['topic'])
        self.assertEqual('nova-compute:compute', dimensions['hostname'])
        self.assertIsNotNone(payload)
        self.assertThat(len(payload), matchers.GreaterThan(0))

    def test_normalize_timestamp(self):
        today = datetime.today().strftime('%Y-%m-%d')
        normalize_timestamp = events_repository.ElasticSearchEventsRepository._normalize_timestamp

        self.assertEqual(today, normalize_timestamp(None))
        self.assertEqual(today, normalize_timestamp(''))
        self.assertEqual(today, normalize_timestamp('foo'))
        self.assertEqual(today, normalize_timestamp('2017-02-3'))
        self.assertEqual(today, normalize_timestamp('2017-02-31'))

        self.assertEqual('2017-08-07', normalize_timestamp('2017-08-07 11:22:43'))

    @mock.patch('monasca_common.kafka.legacy_kafka_message')
    def _load_event(self, event_name, mock_kafka_message):
        if self.events is None:
            filepath = os.path.join(os.path.dirname(__file__), 'events.json')
            self.events = json.load(open(filepath))
        # create a kafka message envelope
        value = json.dumps(self.events[event_name])
        message = mock_kafka_message.LegacyKafkaMessage()
        message.value.return_value = value
        return message
