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

influxdb_opts = [
    cfg.StrOpt('database_name',
               help='database name where metrics are stored',
               default='mon'),
    cfg.HostAddressOpt('ip_address',
                       help='Valid IP address or hostname '
                            'to InfluxDB instance'),
    cfg.PortOpt('port',
                help='port to influxdb',
                default=8086),
    cfg.StrOpt('user',
               help='influxdb user ',
               default='mon_persister'),
    cfg.StrOpt('password',
               secret=True,
               help='influxdb password')]

influxdb_group = cfg.OptGroup(name='influxdb',
                              title='influxdb')


def register_opts(conf):
    conf.register_group(influxdb_group)
    conf.register_opts(influxdb_opts, influxdb_group)


def list_opts():
    return influxdb_group, influxdb_opts
