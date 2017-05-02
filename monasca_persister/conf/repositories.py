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

repositories_opts = [
    cfg.StrOpt(name='metrics_driver',
               help='The repository driver to use for metrics',
               default='monasca_persister.repositories.influxdb.metrics_repository:MetricInfluxdbRepository'),
    cfg.StrOpt(name='alarm_state_history_driver',
               help='The repository driver to use for alarm state history',
               default='monasca_persister.repositories.influxdb.metrics_repository:MetricInfluxdbRepository')]

repositories_group = cfg.OptGroup(name='repositories',
                                  title='repositories')


def register_opts(conf):
    conf.register_group(repositories_group)
    conf.register_opts(repositories_opts, repositories_group)


def list_opts():
    return repositories_group, repositories_opts
