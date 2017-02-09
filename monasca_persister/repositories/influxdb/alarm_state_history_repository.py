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
from monasca_persister.repositories.utils import parse_alarm_state_hist_message

LOG = log.getLogger(__name__)


class AlarmStateHistInfluxdbRepository(
    abstract_repository.AbstractInfluxdbRepository):

    def __init__(self):

        super(AlarmStateHistInfluxdbRepository, self).__init__()

    def process_message(self, message):

        (alarm_id, metrics, new_state, old_state, link,
         lifecycle_state, state_change_reason,
         sub_alarms_json_snake_case, tenant_id,
         time_stamp) = parse_alarm_state_hist_message(
                message)

        name = u'alarm_state_history'
        fields = []
        fields.append(u'tenant_id=' + line_utils.escape_value(tenant_id))
        fields.append(u'alarm_id=' + line_utils.escape_value(alarm_id))
        fields.append(u'metrics=' + line_utils.escape_value(
            json.dumps(metrics, ensure_ascii=False)))
        fields.append(u'new_state=' + line_utils.escape_value(new_state))
        fields.append(u'old_state=' + line_utils.escape_value(old_state))
        fields.append(u'link=' + line_utils.escape_value(link))
        fields.append(u'lifecycle_state=' + line_utils.escape_value(
            lifecycle_state))
        fields.append(u'reason=' + line_utils.escape_value(
            state_change_reason))
        fields.append(u'reason_data=' + line_utils.escape_value("{}"))
        fields.append(u'sub_alarms=' + line_utils.escape_value(
            sub_alarms_json_snake_case))

        line = name + u',tenant_id=' + line_utils.escape_tag(tenant_id)
        line += u' ' + u','.join(fields)
        line += u' ' + str(int(time_stamp))

        LOG.debug(line)

        return line
