# (C) Copyright 2016-2017 Hewlett Packard Enterprise Development LP
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
import json

from oslo_log import log

from monasca_persister.repositories.influxdb import abstract_repository
from monasca_persister.repositories.influxdb import line_utils
from monasca_persister.repositories.utils import parse_measurement_message

LOG = log.getLogger(__name__)


class MetricInfluxdbRepository(abstract_repository.AbstractInfluxdbRepository):

    def __init__(self):

        super(MetricInfluxdbRepository, self).__init__()

    def process_message(self, message):

        (dimensions, metric_name, region, tenant_id, time_stamp, value,
         value_meta) = parse_measurement_message(message)

        tags = dimensions
        tags[u'_tenant_id'] = tenant_id
        tags[u'_region'] = region

        if not value_meta:
            value_meta_str = u'"{}"'
        else:
            value_meta_str = line_utils.escape_value(json.dumps(value_meta, ensure_ascii=False))

        key_values = [line_utils.escape_tag(metric_name)]

        # tags should be sorted client-side to take load off server
        for key in sorted(tags.keys()):
            key_tag = line_utils.escape_tag(key)
            value_tag = line_utils.escape_tag(tags[key])
            key_values.append(key_tag + u'=' + value_tag)
        key_values = u','.join(key_values)

        value_field = u'value={}'.format(value)
        value_meta_field = u'value_meta=' + value_meta_str

        data = key_values + u' ' + value_field + u',' + value_meta_field + u' ' + str(int(time_stamp))

        LOG.debug(data)

        return data
