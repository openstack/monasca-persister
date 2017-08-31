# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
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

import abc
from oslo_config import cfg
import six

from monasca_persister.repositories import abstract_repository
from monasca_persister.repositories.cassandra import connection_util

conf = cfg.CONF

@six.add_metaclass(abc.ABCMeta)
class AbstractCassandraRepository(abstract_repository.AbstractRepository):
    def __init__(self):
        super(AbstractCassandraRepository, self).__init__()

        self._cluster = connection_util.create_cluster()
        self._session = connection_util.create_session(self._cluster)
        self._retention = conf.cassandra.retention_policy * 24 * 3600
        self._cache_size = conf.cassandra.max_definition_cache_size
        self._max_batches = conf.cassandra.max_batches
