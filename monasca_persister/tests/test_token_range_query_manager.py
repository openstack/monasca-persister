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

from monasca_persister.repositories.cassandra.token_range_query_manager \
    import TokenRangeQueryManager


class FakeException(Exception):
    pass


class TestTokenRangeQueryManager(base.BaseTestCase):

    def setUp(self):
        super(TestTokenRangeQueryManager, self).setUp()
        self._set_patchers()
        self._set_mocks()

        cql, result_handler = mock.Mock, mock.Mock()
        self.token_range_query_mgr = TokenRangeQueryManager(cql, result_handler, process_count=1)

    def _set_patchers(self):
        self.patcher_setup = mock.patch.object(TokenRangeQueryManager,
                                               '_setup',
                                               return_value=None)

    def _set_mocks(self):
        self.mock_setup = self.patcher_setup.start()

    def tearDown(self):
        super(TestTokenRangeQueryManager, self).tearDown()
        self.mock_setup.reset_mock()
        self.patcher_setup.stop()

    def test_close_pool(self):
        with mock.patch.object(self.token_range_query_mgr._pool, 'join',
                               side_effect=None):
            self.assertIsNone(self.token_range_query_mgr.close_pool())

    def test_query(self):
        with mock.patch.object(self.token_range_query_mgr._pool, 'map',
                               side_effect=FakeException):
            sample_element = mock.Mock()
            sample_element.value = 1
            token_ring = [sample_element, sample_element]
            self.assertRaises(FakeException, self.token_range_query_mgr.query, token_ring)
