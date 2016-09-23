# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
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
from datetime import datetime
import json

from oslo_log import log
import pytz

from monasca_persister.repositories.influxdb import abstract_repository
from monasca_persister.repositories.utils import parse_measurement_message

LOG = log.getLogger(__name__)


class MetricInfluxdbRepository(abstract_repository.AbstractInfluxdbRepository):

    def __init__(self):

        super(MetricInfluxdbRepository, self).__init__()

    def process_message(self, message):

        (dimensions, metric_name, region, tenant_id, time_stamp, value,
         value_meta) = parse_measurement_message(message)

        tags = dimensions
        tags['_tenant_id'] = tenant_id.encode('utf8')
        tags['_region'] = region.encode('utf8')

        ts = time_stamp / 1000.0

        data = {"measurement": metric_name.encode('utf8'),
                "time": datetime.fromtimestamp(ts, tz=pytz.utc).strftime(
                        '%Y-%m-%dT%H:%M:%S.%fZ'),
                "fields": {
                    "value": value,
                    "value_meta": json.dumps(value_meta,
                                             ensure_ascii=False).encode('utf8')
                },
                "tags": tags}

        LOG.debug(data)

        return data
