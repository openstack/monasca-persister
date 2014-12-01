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

"""Persister Module

   The Persister reads metrics and alarms from Kafka and then stores them
   in InfluxDB.

   Start the perister as stand-alone process by running 'persister.py
   --config-file <config file>'

   Also able to use Openstack service to start the persister.

"""

import abc
from datetime import datetime
import json
import os
import six
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

kafka_common_opts = [cfg.StrOpt('uri'),
                     cfg.StrOpt('group_id'),
                     cfg.StrOpt('topic'),
                     cfg.StrOpt('consumer_id'),
                     cfg.StrOpt('client_id'),
                     cfg.IntOpt('database_batch_size'),
                     cfg.IntOpt('max_wait_time_seconds'),
                     cfg.IntOpt('fetch_size_bytes'),
                     cfg.IntOpt('buffer_size'),
                     cfg.IntOpt('max_buffer_size')]

kafka_metrics_opts = kafka_common_opts
kafka_alarm_history_opts = kafka_common_opts

kafka_metrics_group = cfg.OptGroup(name='kafka_metrics', title='kafka_metrics')
kafka_alarm_history_group = cfg.OptGroup(name='kafka_alarm_history',
                                         title='kafka_alarm_history')

cfg.CONF.register_group(kafka_metrics_group)
cfg.CONF.register_group(kafka_alarm_history_group)
cfg.CONF.register_opts(kafka_metrics_opts, kafka_metrics_group)
cfg.CONF.register_opts(kafka_alarm_history_opts, kafka_alarm_history_group)

influxdb_opts = [cfg.StrOpt('database_name'),
                 cfg.StrOpt('ip_address'),
                 cfg.StrOpt('port'),
                 cfg.StrOpt('user'),
                 cfg.StrOpt('password')]

influxdb_group = cfg.OptGroup(name='influxdb', title='influxdb')
cfg.CONF.register_group(influxdb_group)
cfg.CONF.register_opts(influxdb_opts, influxdb_group)

cfg.CONF(sys.argv[1:])

log_levels = (cfg.CONF.default_log_levels)
cfg.set_defaults(log.log_opts, default_log_levels=log_levels)
log.setup("monasca-persister")


def main():
    """Start persister.

    Start metric persister and alarm persister in separate threads.
    """

    metric_persister = MetricPersister(cfg.CONF.kafka_metrics,
                                       cfg.CONF.influxdb)
    alarm_persister = AlarmPersister(cfg.CONF.kafka_alarm_history,
                                     cfg.CONF.influxdb)

    metric_persister.start()
    alarm_persister.start()

    LOG.info('''

               _____
              /     \   ____   ____ _____    ______ ____ _____
             /  \ /  \ /  _ \ /    \\\__  \  /  ___// ___\\\__  \\
            /    Y    (  <_> )   |  \/ __ \_\___ \\  \___ / __  \\_
            \____|__  /\____/|___|  (____  /____  >\___  >____  /
                    \/            \/     \/     \/     \/     \/
            __________                    .__          __
            \______   \ ___________  _____|__| _______/  |_  ___________
             |     ___// __ \_  __ \/  ___/  |/  ___/\   __\/ __ \_  __ \\
             |    |   \  ___/|  | \/\___ \|  |\___ \  |  | \  ___/|  | \/
             |____|    \___  >__|  /____  >__/____  > |__|  \___  >__|
                           \/           \/        \/            \/

        ''')

    LOG.info('Monasca Persister has started successfully!')


def shutdown_all_threads_and_die():
    """Shut down all threads and exit process.

    Hit it with a hammer to kill all threads and die. May cause duplicate
    messages in kafka queue to be reprocessed when the persister starts again.
    Happens if the persister dies just after sending metrics and alarms to the
    DB but does notreach the commit.
    """

    os._exit(1)


class Persister(os_service.Service):
    """Class used with Openstack service.
    """

    def __init__(self, threads=1):
            super(Persister, self).__init__(threads)

    def start(self):

        try:

            main()

        except:
            LOG.exception('Persister encountered fatal error. '
                          'Shutting down all threads and exiting.')
            shutdown_all_threads_and_die()


@six.add_metaclass(abc.ABCMeta)
class AbstractPersister(threading.Thread):
    def __init__(self, kafka_conf, influxdb_conf):

        super(AbstractPersister, self).__init__()

        kafka = KafkaClient(kafka_conf.uri)
        self._consumer = (
            SimpleConsumer(kafka,
                           kafka_conf.group_id,
                           kafka_conf.topic,
                           # Set to true even though we actually do
                           # the commits manually. Needed to
                           # initialize
                           # offsets correctly.
                           auto_commit=True,
                           # Make these values None so that the
                           # manual commit will do the actual
                           # commit.
                           # Needed so that offsets are initialized
                           # correctly. If not done, then restarts
                           # will reread messages from beginning of
                           # the queue.
                           auto_commit_every_n=None,
                           auto_commit_every_t=None,
                           fetch_size_bytes=kafka_conf.fetch_size_bytes,
                           buffer_size=kafka_conf.buffer_size,
                           max_buffer_size=kafka_conf.max_buffer_size,
                           iter_timeout=1))

        self._influxdb_client = InfluxDBClient(influxdb_conf.ip_address,
                                               influxdb_conf.port,
                                               influxdb_conf.user,
                                               influxdb_conf.password,
                                               influxdb_conf.database_name)

        self._max_wait_time_secs = kafka_conf.max_wait_time_seconds
        self._database_batch_size = kafka_conf.database_batch_size
        self._kafka_topic = kafka_conf.topic

        self._json_body = []
        self._last_flush = datetime.now()

    @abc.abstractmethod
    def process_message(self, message):
        pass

    def _flush(self):

        if self._json_body:
            self._influxdb_client.write_points(self._json_body)
            self._consumer.commit()
            LOG.info("processed {} messages from topic '{}'".format(
                len(self._json_body), self._kafka_topic))
            self._json_body = []
        self._last_flush = datetime.now()

    def run(self):

        try:

            while True:

                delta_time = datetime.now() - self._last_flush
                if delta_time.seconds > self._max_wait_time_secs:
                    self._flush()

                for message in self._consumer:
                    try:
                        self._json_body.append(self.process_message(message))
                    except Exception:
                        LOG.exception('Error processing message. Message is '
                                      'being dropped. {}'.format(message))
                    if len(self._json_body) >= self._database_batch_size:
                        self._flush()

        except:
            LOG.exception(
                'Persister encountered fatal exception processing messages. '
                'Shutting down all threads and exiting')
            shutdown_all_threads_and_die()


class AlarmPersister(AbstractPersister):
    """Class for persisting alarms.
    """

    def __init__(self, kafka_conf, influxdb_conf):

        super(AlarmPersister, self).__init__(kafka_conf, influxdb_conf)

    def process_message(self, message):

        LOG.debug(message.message.value.decode('utf8'))

        decoded = json.loads(message.message.value)
        LOG.debug(json.dumps(decoded, sort_keys=True, indent=4))

        alarm_transitioned = decoded['alarm-transitioned']

        actions_enabled = alarm_transitioned['actionsEnabled']
        LOG.debug('actions enabled: %s', actions_enabled)

        alarm_description = alarm_transitioned['alarmDescription']
        LOG.debug('alarm description: %s', alarm_description)

        alarm_id = alarm_transitioned['alarmId']
        LOG.debug('alarm id: %s', alarm_id)

        alarm_definition_id = alarm_transitioned[
            'alarmDefinitionId']
        LOG.debug('alarm definition id: %s', alarm_definition_id)

        metrics = alarm_transitioned['metrics']
        LOG.debug('metrics: %s', metrics)

        alarm_name = alarm_transitioned['alarmName']
        LOG.debug('alarm name: %s', alarm_name)

        new_state = alarm_transitioned['newState']
        LOG.debug('new state: %s', new_state)

        old_state = alarm_transitioned['oldState']
        LOG.debug('old state: %s', old_state)

        state_change_reason = alarm_transitioned[
            'stateChangeReason']
        LOG.debug('state change reason: %s', state_change_reason)

        tenant_id = alarm_transitioned['tenantId']
        LOG.debug('tenant id: %s', tenant_id)

        time_stamp = alarm_transitioned['timestamp']
        LOG.debug('time stamp: %s', time_stamp)

        data = {"points": [[time_stamp,
                            '{}',
                            tenant_id.encode('utf8'),
                            alarm_id.encode('utf8'),
                            alarm_definition_id.encode('utf8'),
                            json.dumps(metrics, ensure_ascii=False).encode(
                                'utf8'),
                            old_state.encode('utf8'),
                            new_state.encode('utf8'),
                            state_change_reason.encode('utf8')]],
                "name": 'alarm_state_history',
                "columns": ["time",
                            "reason_data",
                            "tenant_id",
                            "alarm_id",
                            "alarm_definition_id",
                            "metrics",
                            "old_state",
                            "new_state",
                            "reason"]}

        LOG.debug(data)

        return data


class MetricPersister(AbstractPersister):
    """Class for persisting metrics.
    """

    def __init__(self, kafka_conf, influxdb_conf):

        super(MetricPersister, self).__init__(kafka_conf, influxdb_conf)

    def process_message(self, message):

        LOG.debug(message.message.value.decode('utf8'))

        decoded = json.loads(message.message.value)
        LOG.debug(json.dumps(decoded, sort_keys=True, indent=4))

        metric = decoded['metric']

        metric_name = metric['name']
        LOG.debug('name: %s', metric_name)

        creation_time = decoded['creation_time']
        LOG.debug('creation time: %s', creation_time)

        region = decoded['meta']['region']
        LOG.debug('region: %s', region)

        tenant_id = decoded['meta']['tenantId']
        LOG.debug('tenant id: %s', tenant_id)

        dimensions = {}
        if 'dimensions' in metric:
            for dimension_name in metric['dimensions']:
                dimensions[dimension_name] = (
                    metric['dimensions'][dimension_name])
                LOG.debug('dimension: %s : %s', dimension_name,
                          dimensions[dimension_name])

        time_stamp = metric['timestamp']
        LOG.debug('timestamp %s', time_stamp)

        value = metric['value']
        LOG.debug('value: %s', value)

        url_encoded_serie_name = (
            urllib.quote(metric_name.encode('utf8'),
                         safe='') + '?' + urllib.quote(
                tenant_id.encode('utf8'), safe='') + '&' + urllib.quote(
                region.encode('utf8'), safe=''))

        for dimension_name in dimensions:
            url_encoded_serie_name += (
                '&' + urllib.quote(dimension_name.encode('utf8'),
                                   safe='') + '=' + urllib.quote(
                    dimensions[dimension_name].encode('utf8'), safe=''))

        LOG.debug("url_encoded_serie_name: %s", url_encoded_serie_name)

        data = {"points": [[value,
                            time_stamp]],
                "name": url_encoded_serie_name,
                "columns": ["value",
                            "time"]}

        LOG.debug(data)

        return data


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
