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

import os
from unittest import mock

from oslotest import base
from oslo_config import cfg

from monasca_common.kafka import consumer
from monasca_persister.kafka.legacy_kafka_persister import LegacyKafkaPersister
from monasca_persister.repositories import data_points
from monasca_persister.repositories.persister import LOG


class FakeException(Exception):
    pass


class TestPersisterRepo(base.BaseTestCase):
    def setUp(self):
        super(TestPersisterRepo, self).setUp()

        self._set_patchers()
        self._set_mocks()

        self.persister = LegacyKafkaPersister(self.mock_kafka,
                                              self.mock_zookeeper, mock.Mock())

    def _set_mocks(self):
        self.mock_kafka = mock.Mock()
        self.mock_kafka.topic = 'topic'
        self.mock_kafka.batch_size = 1
        self.mock_kafka.zookeeper_path = ''
        self.mock_kafka.group_id = 0
        self.mock_kafka.max_wait_time_seconds = 0
        self.mock_zookeeper = mock.Mock(uri='')

        self.mock_consumer_init = self.patch_kafka_init.start()
        self.mock_client_init = self.patch_kafka_client_init.start()
        self.mock_consumer_commit = self.patch_kafka_commit.start()
        self.mock_log_warning = self.patch_log_warning.start()
        self.mock_log_exception = self.patch_log_exception.start()

    def _set_patchers(self):
        self.patch_kafka_init = mock.patch.object(consumer.KafkaConsumer,
                                                  '__init__',
                                                  return_value=None)
        self.patch_kafka_commit = \
            mock.patch.object(consumer.KafkaConsumer, 'commit',
                              return_value=FakeException())
        self.patch_kafka_client_init = \
            mock.patch.object(consumer.kafka_client.KafkaClient, '__init__',
                              return_value=None)
        self.patch_log_warning = mock.patch.object(LOG, 'warning')
        self.patch_log_exception = mock.patch.object(LOG, 'exception')

    def tearDown(self):
        super(TestPersisterRepo, self).tearDown()

        self.mock_consumer_init.reset_mock()
        self.mock_client_init.reset_mock()
        self.mock_consumer_commit.reset_mock()
        self.mock_log_warning.reset_mock()
        self.mock_log_exception.reset_mock()

        self.patch_kafka_init.stop()
        self.patch_kafka_commit.stop()
        self.patch_kafka_client_init.stop()
        self.patch_log_warning.stop()
        self.patch_log_exception.stop()

    def test_flush_if_data_points_is_none(self):
        self.persister._data_points = None
        self.assertIsNone(self.persister._flush())

    def test_run_if_consumer_is_faulty(self):
        with mock.patch.object(os, '_exit', return_value=None) as mock_exit:
            self.persister._data_points = data_points.DataPointsAsDict()
            self.persister._consumer = mock.Mock(side_effect=FakeException)
            self.persister.run()
            mock_exit.assert_called_once_with(1)

    def test_run_logs_exception_from_consumer(self):
        with mock.patch.object(self.persister.repository, 'process_message',
                               side_effect=FakeException):
            self.persister._data_points = data_points.DataPointsAsDict()
            self.persister._consumer = ['aa']
            self.persister.run()
            self.mock_log_exception.assert_called()

    def test_run_commit_is_called_and_data_points_is_emptied(self):
        with mock.patch.object(self.persister.repository, 'process_message',
                               return_value=('message', 'tenant_id')):
            with mock.patch.object(self.persister, '_consumer',
                                   return_value=mock.Mock()) as mock_consumer:
                self.persister._data_points = data_points.DataPointsAsDict()
                self.persister._data_points.append('fake_tenant_id', 'some')
                self.persister._consumer.__iter__.return_value = ('aa', 'bb')
                self.persister._batch_size = 1
                self.persister.run()
                mock_consumer.commit.assert_called()
                self.assertEqual(0, self.persister._data_points.counter)

    def test_flush_logs_warning_and_exception(self):
        exception_msgs = ['partial write: points beyond retention policy dropped',
                          'unable to parse']
        with(mock.patch.object(cfg.CONF.repositories,
                               'ignore_parse_point_error', return_value=True)):
            for elem in exception_msgs:
                with mock.patch.object(LOG, 'info', side_effect=FakeException(
                        elem)):
                    self.persister._data_points = data_points.DataPointsAsDict()
                    self.persister._data_points.append('fake_tenant_id', 'some')
                    self.persister._flush()
                    self.mock_log_warning.assert_called()

    @mock.patch.object(LOG, 'info', side_effect=FakeException())
    def test_flush_logs_exception(self, mock_log_info):
        with(mock.patch.object(cfg.CONF.repositories,
                               'ignore_parse_point_error',
                               return_value=False)):
            mock_log_info.side_effect.message = 'some msg'
            self.persister._data_points = data_points.DataPointsAsDict()
            self.persister._data_points.append('fake_tenant_id', 'some')
            self.assertRaises(FakeException, self.persister._flush)
            self.mock_log_exception.assert_called()
