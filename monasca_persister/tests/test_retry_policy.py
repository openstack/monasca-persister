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

from cassandra.policies import RetryPolicy
from oslotest import base

from monasca_persister.repositories.cassandra import retry_policy


class TestMonascaRetryPolicy(base.BaseTestCase):
    def setUp(self):
        super(TestMonascaRetryPolicy, self).setUp()
        self.monasca_retry_policy = retry_policy.MonascaRetryPolicy(0, 0, 0)

    def tearDown(self):
        super(TestMonascaRetryPolicy, self).tearDown()

    def test_on_read_timeout_when_retry_num_gt_read_attempts(self):
        self.monasca_retry_policy.read_attempts = 0
        retry_num = 1
        rethrow, none = self.monasca_retry_policy.on_read_timeout(mock.Mock(),
                                                                  mock.Mock(),
                                                                  mock.Mock(),
                                                                  0, 0,
                                                                  retry_num)
        self.assertEqual(rethrow, RetryPolicy.RETHROW)
        self.assertEqual(none, None)

    def test_on_read_timeout_when_responses_received_gt_required_and_data_retrieved_is_none(self):
        consistency = 'some_value'
        received_responses = 1
        required_responses = 0
        data_retrieved = None
        retry_num = 0
        self.monasca_retry_policy.read_attempts = 1
        returned_retry, returned_consistency = self.monasca_retry_policy. \
            on_read_timeout(mock.Mock(), consistency, required_responses,
                            received_responses, data_retrieved, retry_num)
        self.assertEqual(returned_retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, returned_consistency)

    def test_on_read_timeout_if_previous_conditions_not_fulfilled(self):
        consistency = 'some_value'
        received_responses = 1
        required_reponses = 0
        data_retrieved = ['some']
        retry_num = 0
        self.monasca_retry_policy.read_attempts = 1
        returned_rethrow, returned_none = self.monasca_retry_policy. \
            on_read_timeout(mock.Mock(), consistency, required_reponses,
                            received_responses, data_retrieved, retry_num)
        self.assertEqual(returned_rethrow, RetryPolicy.RETHROW)
        self.assertEqual(returned_none, None)

    def test_on_write_timeout_if_retry_num_gt_or_eq_write_attempts(self):
        retry_num = 1
        self.monasca_retry_policy.write_attempts = 0
        returned_rethrow, returned_none = self.monasca_retry_policy. \
            on_write_timeout(mock.Mock(), mock.Mock(), mock.Mock(), 0, 0,
                             retry_num)
        self.assertEqual(returned_rethrow, RetryPolicy.RETHROW)
        self.assertEqual(returned_none, None)

    def test_on_write_timeout_if_retry_num_lt_write_attempts(self):
        retry_num = 0
        consistency = 0
        self.monasca_retry_policy.write_attempts = 1
        returned_retry, returned_consistency = self.monasca_retry_policy. \
            on_write_timeout(mock.Mock(), consistency, mock.Mock(), 0, 0,
                             retry_num)
        self.assertEqual(returned_retry, RetryPolicy.RETRY)
        self.assertEqual(returned_consistency, consistency)

    def test_on_unavailable_if_retry_num_lt_unavailable_attempts(self):
        consistency = 0
        retry_num = 0
        self.monasca_retry_policy.unavailable_attempts = 1

        returned_retry_next_host, returned_consistency = \
            self.monasca_retry_policy.on_unavailable(mock.Mock(),
                                                     consistency, 0, 0, retry_num)
        self.assertEqual(returned_consistency, consistency)
        self.assertEqual(returned_retry_next_host, RetryPolicy.RETRY_NEXT_HOST)

    def test_on_unavailable_if_retry_num_gt_or_eq_unavailable_attempts(self):
        consistency = 0
        retry_num = 1
        self.monasca_retry_policy.unavailable_attempts = 1

        returned_rethrow, returned_none = \
            self.monasca_retry_policy.on_unavailable(mock.Mock(),
                                                     consistency, 0, 0, retry_num)
        self.assertEqual(returned_none, None)
        self.assertEqual(returned_rethrow, RetryPolicy.RETHROW)
