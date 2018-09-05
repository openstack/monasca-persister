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

elasticsearch_opts = [
    cfg.StrOpt(
        'index_name',
        help='Index prefix name where events are stored',
        default='events'),
    cfg.ListOpt(
        'hosts',
        help='List of Elasticsearch nodes in format host[:port]',
        default=['localhost:9200'],
        item_type=types.HostAddressPortType()),
    cfg.BoolOpt(
        'sniff_on_start',
        help='Flag indicating whether to obtain a list of nodes from the cluser at startup time',
        default=False),
    cfg.BoolOpt(
        'sniff_on_connection_fail',
        help='Flag controlling if connection failure triggers a sniff',
        default=False),
    cfg.IntOpt(
        'sniffer_timeout',
        help='Number of seconds between automatic sniffs',
        default=None),
    cfg.IntOpt(
        'max_retries',
        help='Maximum number of retries before an exception is propagated',
        default=3,
        min=1)]

elasticsearch_group = cfg.OptGroup(name='elasticsearch', title='elasticsearch')


def register_opts(conf):
    conf.register_group(elasticsearch_group)
    conf.register_opts(elasticsearch_opts, elasticsearch_group)


def list_opts():
    return elasticsearch_group, elasticsearch_opts
