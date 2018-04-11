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

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.cluster import ConsistencyLevel
from cassandra.cluster import DCAwareRoundRobinPolicy
from cassandra.cluster import TokenAwarePolicy
from oslo_config import cfg

from monasca_persister.repositories.cassandra.retry_policy import MonascaRetryPolicy

conf = cfg.CONF

def create_cluster():
    user = conf.cassandra.user
    if user:
        auth_provider = PlainTextAuthProvider(username=user, password=conf.cassandra.password)
    else:
        auth_provider = None

    contact_points = [ip.dest for ip in conf.cassandra.contact_points]
    cluster = Cluster(contact_points,
                      port=conf.cassandra.port,
                      auth_provider=auth_provider,
                      connect_timeout=conf.cassandra.connection_timeout,
                      load_balancing_policy=TokenAwarePolicy(
                          DCAwareRoundRobinPolicy(local_dc=conf.cassandra.local_data_center)),
                      )
    cluster.default_retry_policy = MonascaRetryPolicy(1, conf.cassandra.max_write_retries,
                                                      conf.cassandra.max_write_retries)
    return cluster


def create_session(cluster):
    session = cluster.connect(conf.cassandra.keyspace)
    session.default_timeout = conf.cassandra.read_timeout
    session.default_consistency_level = \
        ConsistencyLevel.name_to_value[conf.cassandra.consistency_level]
    return session
