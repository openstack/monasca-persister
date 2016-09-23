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
import json

from cassandra import query
from oslo_log import log

from monasca_persister.repositories.cassandra import abstract_repository
from monasca_persister.repositories.utils import parse_alarm_state_hist_message

LOG = log.getLogger(__name__)


class AlarmStateHistCassandraRepository(
    abstract_repository.AbstractCassandraRepository):

    def __init__(self):

        super(AlarmStateHistCassandraRepository, self).__init__()

        self._insert_alarm_state_hist_stmt = self.cassandra_session.prepare(
                'insert into alarm_state_history (tenant_id, alarm_id, '
                'metrics, new_state, '
                'old_state, reason, reason_data, '
                'sub_alarms, time_stamp) values (?,?,?,?,?,?,?,?,?)')

    def process_message(self, message):

        (alarm_id, metrics, new_state, old_state, link,
         lifecycle_state, state_change_reason,
         sub_alarms_json_snake_case, tenant_id,
         time_stamp) = parse_alarm_state_hist_message(
                message)

        alarm_state_hist = (
            tenant_id.encode('utf8'),
            alarm_id.encode('utf8'),
            json.dumps(metrics, ensure_ascii=False).encode(
                    'utf8'),
            new_state.encode('utf8'),
            old_state.encode('utf8'),
            state_change_reason.encode('utf8'),
            "{}".encode('utf8'),
            sub_alarms_json_snake_case.encode('utf8'),
            time_stamp
        )

        LOG.debug(alarm_state_hist)

        return alarm_state_hist

    def write_batch(self, alarm_state_hists):

        for alarm_state_hist in alarm_state_hists:
            self._batch_stmt.add(self._insert_alarm_state_hist_stmt,
                                 alarm_state_hist)

        self.cassandra_session.execute(self._batch_stmt)

        self._batch_stmt = query.BatchStatement()
