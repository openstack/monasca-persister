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

from mock import Mock
from mock import patch

from oslotest import base
from oslo_config import cfg

from monasca_persister.repositories.influxdb.metrics_repository import MetricInfluxdbRepository


class TestMetricInfluxdbRepository(base.BaseTestCase):

    def setUp(self):
        super(TestMetricInfluxdbRepository, self).setUp()

    def tearDown(self):
        super(TestMetricInfluxdbRepository, self).tearDown()

    def test_process_message(self):
        metric = self._get_metric()
        with patch.object(cfg, 'CONF', return_value=None):
            metric_repo = MetricInfluxdbRepository()
            self.assertIsInstance(metric_repo.process_message(metric), tuple)

    def _get_metric(self):
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
                  "tenantId":"df4c002353de4399b92fa79d8374819b"
                },
                "creation_time":1554725988
            }
        '''
        message = Mock()
        message.value.return_value = metric
        return message
