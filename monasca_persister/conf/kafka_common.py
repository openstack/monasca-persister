# (C) Copyright 2016-2017 Hewlett Packard Enterprise Development LP
# Copyright 2017 FUJITSU LIMITED
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

from oslo_config import cfg


kafka_common_opts = [
    cfg.StrOpt('consumer_id',
               help='Name/id of persister kafka consumer',
               advanced=True,
               default='monasca-persister'),
    cfg.StrOpt('client_id',
               help='id of persister kafka client',
               advanced=True,
               default='monasca-persister'),
    cfg.IntOpt('database_batch_size',
               help='Maximum number of metric to buffer before writing to database',
               default=1000),
    cfg.IntOpt('max_wait_time_seconds',
               help='Maximum wait time for write batch to database',
               default=30),
    cfg.IntOpt('fetch_size_bytes',
               help='Fetch size, in bytes. This value is set to the kafka-python defaults',
               default=4096),
    cfg.IntOpt('buffer_size',
               help='Buffer size, in bytes. This value is set to the kafka-python defaults',
               default='$kafka.fetch_size_bytes'),
    cfg.IntOpt('max_buffer_size',
               help='Maximum buffer size, in bytes, default value is 8 time buffer_size.'
                    'This value is set to the kafka-python defaults. ',
               default=32768),
    cfg.IntOpt('num_processors',
               help='Number of processes spawned by persister',
               default=0)
]

kafka_common_group = cfg.OptGroup(name='kafka',
                                  title='kafka')


def register_opts(conf):
    conf.register_group(kafka_common_group)
    conf.register_opts(kafka_common_opts, kafka_common_group)


def list_opts():
    return kafka_common_group, kafka_common_opts
