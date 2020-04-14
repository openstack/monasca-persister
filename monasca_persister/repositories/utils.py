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
import simplejson as json


def parse_measurement_message(message):

    decoded_message = json.loads(message.value())

    metric = decoded_message['metric']

    metric_name = metric['name']

    region = decoded_message['meta']['region']

    tenant_id = decoded_message['meta']['tenantId']

    time_stamp = metric['timestamp']

    value = float(metric['value'])

    value_meta = metric.get('value_meta', {})
    value_meta = {} if value_meta is None else value_meta

    return (metric.get('dimensions', {}), metric_name, region, tenant_id,
            time_stamp, value, value_meta)


def parse_alarm_state_hist_message(message):

    decoded_message = json.loads(message.value())

    alarm_transitioned = decoded_message['alarm-transitioned']

    alarm_id = alarm_transitioned['alarmId']

    metrics = alarm_transitioned['metrics']

    new_state = alarm_transitioned['newState']

    old_state = alarm_transitioned['oldState']

    # Key may not exist or value may be none, convert both to ""
    link = alarm_transitioned.get('link', "")
    link = "" if link is None else link

    # Key may not exist or value may be none, convert both to ""
    lifecycle_state = alarm_transitioned.get('lifecycleState', "")
    lifecycle_state = "" if lifecycle_state is None else lifecycle_state

    state_change_reason = alarm_transitioned['stateChangeReason']

    tenant_id = alarm_transitioned['tenantId']

    time_stamp = alarm_transitioned['timestamp']

    sub_alarms = alarm_transitioned['subAlarms']
    if sub_alarms:
        sub_alarms_json = json.dumps(sub_alarms, ensure_ascii=False)

        sub_alarms_json_snake_case = sub_alarms_json.replace(
            '"subAlarmExpression":',
            '"sub_alarm_expression":')

        sub_alarms_json_snake_case = sub_alarms_json_snake_case.replace(
            '"currentValues":',
            '"current_values":')

        # jobrs: I do not think that this shows up
        sub_alarms_json_snake_case = sub_alarms_json_snake_case.replace(
            '"metricDefinition":',
            '"metric_definition":')

        sub_alarms_json_snake_case = sub_alarms_json_snake_case.replace(
            '"subAlarmState":',
            '"sub_alarm_state":')
    else:
        sub_alarms_json_snake_case = "[]"

    return (alarm_id, metrics, new_state, old_state, link,
            lifecycle_state, state_change_reason,
            sub_alarms_json_snake_case, tenant_id, time_stamp)


def parse_events_message(message):

    decoded_message = json.loads(message.value())
    event_type = decoded_message['event']['event_type']
    timestamp = decoded_message['event']['timestamp']
    payload = decoded_message['event']['payload']
    project_id = decoded_message['meta']['project_id']
    dimensions = decoded_message['event']['dimensions']

    return (project_id, timestamp, event_type, payload, dimensions), project_id
