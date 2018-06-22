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

from copy import deepcopy

from oslo_config import cfg

from monasca_persister.conf import kafka_common
from monasca_persister.conf import types

kafka_events_group = cfg.OptGroup(name='kafka_events',
                                  title='kafka_events')
kafka_events_opts = [
    cfg.BoolOpt('enabled',
                help='Enable event persister',
                default=False),
    cfg.ListOpt('uri',
                help='Comma separated list of Kafka broker host:port',
                default=['127.0.0.1:9092'],
                item_type=types.HostAddressPortType()),
    cfg.StrOpt('group_id',
               help='Kafka Group from which persister get data',
               default='1_events'),
    cfg.StrOpt('topic',
               help='Kafka Topic from which persister get data',
               default='monevents'),
    cfg.StrOpt('zookeeper_path',
               help='Path in zookeeper for kafka consumer group partitioning algorithm',
               default='/persister_partitions/$kafka_events.topic'),
]

# Replace Default OPT with reference to kafka group option
kafka_common_opts = deepcopy(kafka_common.kafka_common_opts)
for opt in kafka_common_opts:
    opt.default = '$kafka.{}'.format(opt.name)


def register_opts(conf):
    conf.register_group(kafka_events_group)
    conf.register_opts(kafka_events_opts + kafka_common_opts,
                       kafka_events_group)


def list_opts():
    return kafka_events_group, kafka_events_opts + kafka_common_opts
