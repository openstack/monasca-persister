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

import ujson

from datetime import datetime
from elasticsearch import Elasticsearch
from oslo_config import cfg
from oslo_log import log

from monasca_persister.repositories import abstract_repository
from monasca_persister.repositories import utils

LOG = log.getLogger(__name__)


class ElasticSearchEventsRepository(abstract_repository.AbstractRepository):
    def __init__(self):
        super(ElasticSearchEventsRepository, self).__init__()
        self.conf = cfg.CONF.elasticsearch
        self.es = Elasticsearch(
            hosts=self.conf.hosts,
            sniff_on_start=self.conf.sniff_on_start,
            sniff_on_connection_fail=self.conf.sniff_on_connection_fail,
            sniffer_timeout=self.conf.sniffer_timeout,
            max_retries=self.conf.max_retries
        )

    def process_message(self, message):
        return utils.parse_events_message(message)

    def write_batch(self, data_points):
        for data_point in data_points:
            (tenant_id, timestamp, event_type, payload) = data_point

            index = '%s-%s-%s' % (self.conf.index_name, tenant_id,
                                  ElasticSearchEventsRepository._normalize_timestamp(timestamp))

            body = {
                'tenant_id': tenant_id,
                'timestamp': timestamp,
                'event_type': event_type,
                'payload': payload
            }

            self.es.create(
                index=index,
                doc_type='event',
                body=ujson.dumps(body)
            )

    @staticmethod
    def _normalize_timestamp(timestamp):
        d = None
        if timestamp and len(timestamp) >= 10:
            try:
                d = datetime.strptime(timestamp[0:10], '%Y-%m-%d')
            except ValueError as e:
                LOG.warning("Unable to parse timestamp '%s' - %s" % (timestamp, str(e)))
        if not d:
            d = datetime.today()
        return d.strftime('%Y-%m-%d')
