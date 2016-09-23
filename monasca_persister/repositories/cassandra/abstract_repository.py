# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
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
from cassandra import cluster
from cassandra import query
from oslo_config import cfg
import six

from monasca_persister.repositories import abstract_repository


@six.add_metaclass(abc.ABCMeta)
class AbstractCassandraRepository(abstract_repository.AbstractRepository):

    def __init__(self):
        super(AbstractCassandraRepository, self).__init__()
        self.conf = cfg.CONF

        self._cassandra_cluster = cluster.Cluster(
                self.conf.cassandra.cluster_ip_addresses.split(','))

        self.cassandra_session = self._cassandra_cluster.connect(
                self.conf.cassandra.keyspace)

        self._batch_stmt = query.BatchStatement()
