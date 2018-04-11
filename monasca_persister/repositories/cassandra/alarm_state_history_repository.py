# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
# (C) Copyright 2017 SUSE LLC
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

import ujson as json

from cassandra.concurrent import execute_concurrent_with_args
from oslo_config import cfg
from oslo_log import log

from monasca_persister.repositories.cassandra import abstract_repository
from monasca_persister.repositories.utils import parse_alarm_state_hist_message

LOG = log.getLogger(__name__)

UPSERT_CQL = (
    'update monasca.alarm_state_history USING TTL ? '
    'set metric = ?, old_state = ?, new_state = ?, sub_alarms = ?, reason = ?, reason_data = ? '
    'where tenant_id = ? and alarm_id = ? and time_stamp = ?')


class AlarmStateHistCassandraRepository(abstract_repository.AbstractCassandraRepository):
    def __init__(self):
        super(AlarmStateHistCassandraRepository, self).__init__()

        self._upsert_stmt = self._session.prepare(UPSERT_CQL)

    def process_message(self, message):
        (alarm_id, metrics, new_state, old_state, link, lifecycle_state, state_change_reason,
         sub_alarms_json_snake_case,
         tenant_id, time_stamp) = parse_alarm_state_hist_message(message)

        alarm_state_hist = (self._retention,
                            json.dumps(metrics, ensure_ascii=False).encode('utf8'),
                            old_state.encode('utf8'),
                            new_state.encode('utf8'),
                            sub_alarms_json_snake_case.encode('utf8'),
                            state_change_reason.encode('utf8'),
                            "{}".encode('utf8'),
                            tenant_id.encode('utf8'),
                            alarm_id.encode('utf8'),
                            time_stamp)

        return alarm_state_hist

    def write_batch(self, alarm_state_hists):
        while alarm_state_hists:
            num_rows = min(len(alarm_state_hists), cfg.CONF.kafka_alarm_history.batch_size)
            batch = alarm_state_hists[:num_rows]
            execute_concurrent_with_args(self._session, self._upsert_stmt, batch)
            alarm_state_hists = alarm_state_hists[num_rows:]
