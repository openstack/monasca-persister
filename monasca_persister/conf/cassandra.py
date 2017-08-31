# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
# Copyright 2017 FUJITSU LIMITED
# (C) Copyright 2017 SUSE LLC

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

from oslo_config import cfg

cassandra_opts = [
    cfg.ListOpt('contact_points',
                help='Comma separated list of Cassandra node IP addresses',
                default=['127.0.0.1'],
                item_type=cfg.IPOpt),
    cfg.IntOpt('port',
               help='Cassandra port number',
               default=8086),
    cfg.StrOpt('keyspace',
               help='Keyspace name where metrics are stored',
               default='monasca'),
    cfg.StrOpt('user',
               help='Cassandra user name',
               default=''),
    cfg.StrOpt('password',
               help='Cassandra password',
               secret=True,
               default=''),
    cfg.IntOpt('connection_timeout',
               help='Cassandra timeout in seconds when creating a new connection',
               default=5),
    cfg.IntOpt('read_timeout',
               help='Cassandra read timeout in seconds',
               default=60),
    cfg.IntOpt('max_write_retries',
               help='Maximum number of retries in write ops',
               default=1),
    cfg.IntOpt('max_definition_cache_size',
               help='Maximum number of cached metric definition entries in memory',
               default=20000000),
    cfg.IntOpt('retention_policy',
               help='Data retention period in days',
               default=45),
    cfg.StrOpt('consistency_level',
               help='Cassandra default consistency level',
               default='ONE'),
    cfg.StrOpt('local_data_center',
               help='Cassandra local data center name'),
    cfg.IntOpt('max_batches',
               help='Maximum batch size in Cassandra',
               default=250),
]

cassandra_group = cfg.OptGroup(name='cassandra')


def register_opts(conf):
    conf.register_group(cassandra_group)
    conf.register_opts(cassandra_opts, cassandra_group)


def list_opts():
    return cassandra_group, cassandra_opts
