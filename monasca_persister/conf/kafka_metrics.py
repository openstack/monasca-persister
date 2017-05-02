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

import copy

from oslo_config import cfg

from monasca_persister.conf import kafka_common
from monasca_persister.conf import types

kafka_metrics_group = cfg.OptGroup(name='kafka_metrics',
                                   title='kafka_metrics')
kafka_metrics_opts = [
    # NOTE(czarneckia) default by reference does not work with ListOpt
    cfg.ListOpt('uri',
                help='Comma separated list of Kafka broker host:port',
                default=['127.0.0.1:9092'],
                item_type=types.HostAddressPortType()),
    cfg.StrOpt('group_id',
               help='Kafka Group from which persister get data',
               default='1_metrics'),
    cfg.StrOpt('topic',
               help='Kafka Topic from which persister get data',
               default='metrics'),
    cfg.StrOpt('zookeeper_path',
               help='Path in zookeeper for kafka consumer group partitioning algorithm',
               default='/persister_partitions/$kafka_metrics.topic'),
]

# Replace Default OPt with reference to kafka group option
kafka_common_opts = copy.deepcopy(kafka_common.kafka_common_opts)
for opt in kafka_common_opts:
    opt.default = '$kafka.{}'.format(opt.name)


def register_opts(conf):
    conf.register_group(kafka_metrics_group)
    conf.register_opts(kafka_metrics_opts + kafka_common_opts,
                       kafka_metrics_group)


def list_opts():
    return kafka_metrics_group, kafka_metrics_opts
