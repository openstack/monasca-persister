# (C) Copyright 2017 SUSE LLC
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

import multiprocessing

from oslo_config import cfg
from oslo_log import log

from monasca_persister.repositories.cassandra import connection_util


LOG = log.getLogger(__name__)

conf = cfg.CONF


class TokenRangeQueryManager(object):
    def __init__(self, cql, result_handler, process_count=None):
        if process_count:
            self._process_count = process_count
        else:
            self._process_count = multiprocessing.cpu_count()

        self._pool = multiprocessing.Pool(processes=self._process_count, initializer=self._setup,
                                          initargs=(cql, result_handler,))

    @classmethod
    def _setup(cls, cql, result_handler):
        cls.cluster = connection_util.create_cluster()
        cls.session = connection_util.create_session(cls.cluster)
        cls.prepared = cls.session.prepare(cql)
        cls.result_handler = result_handler

    def close_pool(self):
        self._pool.close()
        self._pool.join()

    def query(self, token_ring):

        range_size = len(token_ring) / self._process_count + 1
        start_index = 0
        params = []
        while start_index < len(token_ring):
            end_index = start_index + range_size - 1
            if end_index >= len(token_ring):
                end_index = len(token_ring) - 1
            params.append((token_ring[start_index].value, token_ring[end_index].value))
            start_index = end_index + 1

        self._pool.map(execute_query_token_range, params, 1)


def execute_query_token_range(token_range):
    results = TokenRangeQueryManager.session.execute(
        TokenRangeQueryManager.prepared.bind(token_range))
    TokenRangeQueryManager.result_handler(results)
