#!/usr/bin/env python
# Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
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

""" Persister
    The Persister reads metrics and alarms from Kafka and then stores them
    in InfluxDB.

    Start the perister as stand-alone process by running 'persister.py
    --config-file <config file>'

    Also able to use Openstack service to start the persister.
"""
from datetime import datetime
import json
import sys
import threading
import urllib

from influxdb import InfluxDBClient
from kafka import KafkaClient
from kafka import SimpleConsumer
from oslo.config import cfg

from openstack.common import log
from openstack.common import service as os_service
import service


LOG = log.getLogger(__name__)

kafka_opts = [
    cfg.StrOpt('uri'),
    cfg.StrOpt('alarm_history_group_id'),
    cfg.StrOpt('alarm_history_topic'),
    cfg.StrOpt('alarm_history_consumer_id'),
    cfg.StrOpt('alarm_history_client_id'),
    cfg.IntOpt('alarm_batch_size'),
    cfg.IntOpt('alarm_max_wait_time_seconds'),
    cfg.StrOpt('metrics_group_id'),
    cfg.StrOpt('metrics_topic'),
    cfg.StrOpt('metrics_consumer_id'),
    cfg.StrOpt('metrics_client_id'),
    cfg.IntOpt('metrics_batch_size'),
    cfg.IntOpt('metrics_max_wait_time_seconds')
]

kafka_group = cfg.OptGroup(name='kafka',
                           title='kafka')

cfg.CONF.register_group(kafka_group)
cfg.CONF.register_opts(kafka_opts, kafka_group)

influxdb_opts = [
    cfg.StrOpt('database_name'),
    cfg.StrOpt('ip_address'),
    cfg.StrOpt('port'),
    cfg.StrOpt('user'),
    cfg.StrOpt('password')
]

influxdb_group = cfg.OptGroup(name='influxdb',
                              title='influxdb')
cfg.CONF.register_group(influxdb_group)
cfg.CONF.register_opts(influxdb_opts, influxdb_group)

cfg.CONF(sys.argv[1:])

log_levels = (cfg.CONF.default_log_levels)
cfg.set_defaults(log.log_opts, default_log_levels=log_levels)
log.setup("monasca-perister")


def main():
    try:
        metric_persister = MetricPersister(cfg.CONF)
        metric_persister.start()

        alarm_persister = AlarmPersister(cfg.CONF)
        alarm_persister.start()
    except Exception:
        log.exception('Persister encountered fatal error. Shutting down.')


class Persister(os_service.Service):
    """Class used with Openstack service.
    """

    def __init__(self, threads=1):
        super(Persister, self).__init__(threads)

    def start(self):
        main()


class AlarmPersister(threading.Thread):
    """Class for persisting alarms.
    """

    def __init__(self, conf):
        threading.Thread.__init__(self)
        self.conf = conf

    def run(self):

        count = 0
        json_body = []
        last_flush = datetime.now()

        try:

            kafka = KafkaClient(self.conf.kafka.uri)
            consumer = SimpleConsumer(kafka,
                                      self.conf.kafka.alarm_history_group_id,
                                      self.conf.kafka.alarm_history_topic,
                                      auto_commit=False, iter_timeout=1)

            influxdb_client = InfluxDBClient(self.conf.influxdb.ip_address,
                                             self.conf.influxdb.port,
                                             self.conf.influxdb.user,
                                             self.conf.influxdb.password,
                                             self.conf.influxdb.database_name)
            while (True):

                delta_time = datetime.now() - last_flush
                if (delta_time.seconds >
                        self.conf.kafka.alarm_max_wait_time_seconds):
                    if json_body:
                        influxdb_client.write_points(json_body)
                        consumer.commit()
                    last_flush = datetime.now()
                    count = 0
                    json_body = []

                for message in consumer:
                    LOG.debug(message.message.value.decode('utf8'))

                    decoded = json.loads(message.message.value)
                    LOG.debug(
                        json.dumps(decoded, sort_keys=True, indent=4))

                    actions_enabled = decoded['alarm-transitioned'][
                        'actionsEnabled']
                    LOG.debug('actions enabled: %s', actions_enabled)

                    alarm_description = decoded['alarm-transitioned'][
                        'alarmDescription']
                    LOG.debug('alarm description: %s', alarm_description)

                    alarm_id = decoded['alarm-transitioned']['alarmId']
                    LOG.debug('alarm id: %s', alarm_id)

                    alarm_definition_id = decoded['alarm-transitioned'][
                        'alarmDefinitionId']
                    LOG.debug('alarm definition id: %s', alarm_definition_id)

                    metrics = decoded['alarm-transitioned']['metrics']
                    LOG.debug('metrics: %s', metrics)

                    alarm_name = decoded['alarm-transitioned']['alarmName']
                    LOG.debug('alarm name: %s', alarm_name)

                    new_state = decoded['alarm-transitioned']['newState']
                    LOG.debug('new state: %s', new_state)

                    old_state = decoded['alarm-transitioned']['oldState']
                    LOG.debug('old state: %s', old_state)

                    state_change_reason = decoded['alarm-transitioned'][
                        'stateChangeReason']
                    LOG.debug('state change reason: %s',
                              state_change_reason)

                    tenant_id = decoded['alarm-transitioned']['tenantId']
                    LOG.debug('tenant id: %s', tenant_id)

                    time_stamp = decoded['alarm-transitioned']['timestamp']
                    LOG.debug('time stamp: %s', time_stamp)

                    data = {"points": [
                        [time_stamp, '{}', tenant_id.encode('utf8'),
                         alarm_id.encode('utf8'),
                         alarm_definition_id.encode('utf8'),
                         json.dumps(metrics, ensure_ascii=False).encode('utf8'), old_state.encode('utf8'),
                         new_state.encode('utf8'),
                         state_change_reason.encode('utf8')]],
                            "name": 'alarm_state_history',
                            "columns": ["time", "reason_data", "tenant_id",
                                        "alarm_id", "alarm_definition_id",
                                        "metrics", "old_state",
                                        "new_state",
                                        "reason"]}

                    LOG.debug(data)
                    json_body.append(data)

                    count += 1
                    if count % self.conf.kafka.alarm_batch_size == 0:
                        influxdb_client.write_points(json_body)
                        consumer.commit()
                        last_flush = datetime.now()
                        count = 0
                        json_body = []

        except Exception:
            LOG.exception(
                'Persister encountered fatal exception processing alarms')
            raise


class MetricPersister(threading.Thread):
    """Class for persisting metrics.
    """

    def __init__(self, conf):
        threading.Thread.__init__(self)
        self.conf = conf

    def run(self):

        count = 0
        json_body = []
        last_flush = datetime.now()

        try:

            kafka = KafkaClient(self.conf.kafka.uri)
            consumer = SimpleConsumer(kafka,
                                      self.conf.kafka.metrics_group_id,
                                      self.conf.kafka.metrics_topic,
                                      auto_commit=False, iter_timeout=1)

            influxdb_client = InfluxDBClient(self.conf.influxdb.ip_address,
                                             self.conf.influxdb.port,
                                             self.conf.influxdb.user,
                                             self.conf.influxdb.password,
                                             self.conf.influxdb.database_name)

            while (True):

                delta_time = datetime.now() - last_flush
                if (delta_time.seconds >
                        self.conf.kafka.metrics_max_wait_time_seconds):
                    if json_body:
                        influxdb_client.write_points(json_body)
                        consumer.commit()
                    last_flush = datetime.now()
                    count = 0
                    json_body = []

                for message in consumer:
                    LOG.debug(message.message.value.decode('utf8'))

                    decoded = json.loads(message.message.value)
                    LOG.debug(
                        json.dumps(decoded, sort_keys=True, indent=4))

                    metric_name = decoded['metric']['name']
                    LOG.debug('name: %s', metric_name)

                    creation_time = decoded['creation_time']
                    LOG.debug('creation time: %s', creation_time)

                    region = decoded['meta']['region']
                    LOG.debug('region: %s', region)

                    tenant_id = decoded['meta']['tenantId']
                    LOG.debug('tenant id: %s', tenant_id)

                    dimensions = {}
                    if 'dimensions' in decoded['metric']:
                        for dimension_name in decoded['metric'][
                            'dimensions']:
                            dimensions[dimension_name] = (
                                decoded['metric']['dimensions'][
                                    dimension_name])
                            LOG.debug('dimension: %s : %s', dimension_name,
                                      dimensions[dimension_name])

                    time_stamp = decoded['metric']['timestamp']
                    LOG.debug('timestamp %s', time_stamp)

                    value = decoded['metric']['value']
                    LOG.debug('value: %s', value)

                    url_encoded_serie_name = (
                        urllib.quote(metric_name.encode('utf8'), safe='')
                        + '?' + urllib.quote(tenant_id.encode('utf8'),
                                             safe='')
                        + '&' + urllib.quote(region.encode('utf8'),
                                             safe=''))

                    for dimension_name in dimensions:
                        url_encoded_serie_name += ('&'
                                                   + urllib.quote(
                            dimension_name.encode('utf8'), safe='')
                                                   + '='
                                                   + urllib.quote(
                            dimensions[dimension_name].encode('utf8'),
                            safe=''))

                    LOG.debug("url_encoded_serie_name: %s",
                              url_encoded_serie_name)

                    data = {"points": [[value, time_stamp]],
                            "name": url_encoded_serie_name,
                            "columns": ["value", "time"]}

                    LOG.debug(data)
                    json_body.append(data)

                    count += 1
                    if count % self.conf.kafka.metrics_batch_size == 0:
                        influxdb_client.write_points(json_body)
                        consumer.commit()
                        last_flush = datetime.now()
                        count = 0
                        json_body = []

        except Exception:
            LOG.exception(
                'Persister encountered fatal exception processing metrics')
            raise


def main_service():
    """Method to use with Openstack service.
    """

    service.prepare_service()
    launcher = os_service.ServiceLauncher()
    launcher.launch_service(Persister())
    launcher.wait()

# Used if run without Openstack service.
if __name__ == "__main__":
    sys.exit(main())