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

from monasca_persister.conf import types

zookeeper_opts = [
    cfg.ListOpt('uri',
                help='Comma separated list of zookeper instance host:port',
                default=['127.0.0.1:2181'],
                item_type=types.HostAddressPortType()),
    cfg.IntOpt('partition_interval_recheck_seconds',
               help='Time between rechecking if partition is available',
               default=15)]

zookeeper_group = cfg.OptGroup(name='zookeeper', title='zookeeper')


def register_opts(conf):
    conf.register_group(zookeeper_group)
    conf.register_opts(zookeeper_opts, zookeeper_group)


def list_opts():
    return zookeeper_group, zookeeper_opts
