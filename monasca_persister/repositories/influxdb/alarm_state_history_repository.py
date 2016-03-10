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

from repositories.influxdb.abstract_repository import AbstractInfluxdbRepository
from repositories.utils import parse_alarm_state_hist_message

LOG = log.getLogger(__name__)


class AlarmStateHistInfluxdbRepository(AbstractInfluxdbRepository):

    def __init__(self):

        super(AlarmStateHistInfluxdbRepository, self).__init__()

    def process_message(self, message):

        (alarm_id, metrics, new_state, old_state, link,
         lifecycle_state, state_change_reason,
         sub_alarms_json_snake_case, tenant_id,
         time_stamp) = parse_alarm_state_hist_message(
                message)

        ts = time_stamp / 1000.0

        data = {"measurement": 'alarm_state_history',
                "time": datetime.fromtimestamp(ts, tz=pytz.utc).strftime(
                        '%Y-%m-%dT%H:%M:%S.%fZ'),
                "fields": {
                    "tenant_id": tenant_id.encode('utf8'),
                    "alarm_id": alarm_id.encode('utf8'),
                    "metrics": json.dumps(metrics, ensure_ascii=False).encode(
                            'utf8'),
                    "new_state": new_state.encode('utf8'),
                    "old_state": old_state.encode('utf8'),
                    "link": link.encode('utf8'),
                    "lifecycle_state": lifecycle_state.encode('utf8'),
                    "reason": state_change_reason.encode('utf8'),
                    "reason_data": "{}".encode('utf8'),
                    "sub_alarms": sub_alarms_json_snake_case.encode('utf8')
                },
                "tags": {
                    "tenant_id": tenant_id.encode('utf8')
                }}

        LOG.debug(data)

        return data
