#!/usr/bin/env python
# (C) Copyright 2014-2016 Hewlett Packard Enterprise Development Company LP
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
import hashlib
import urllib
from datetime import datetime
import json
import os

import six
import sys
import threading

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

from influxdb import InfluxDBClient
import pytz

from oslo_config import cfg
from oslo_log import log
from oslo_service import service as os_service

import service

from monasca_common.kafka.consumer import KafkaConsumer

LOG = log.getLogger(__name__)
log.register_options(cfg.CONF)
log.set_defaults()

database_opts = [cfg.StrOpt('database_type')]

database_group = cfg.OptGroup(name='database')

cfg.CONF.register_group(database_group)
cfg.CONF.register_opts(database_opts, database_group)

zookeeper_opts = [cfg.StrOpt('uri'),
                  cfg.IntOpt('partition_interval_recheck_seconds')]

zookeeper_group = cfg.OptGroup(name='zookeeper', title='zookeeper')
cfg.CONF.register_group(zookeeper_group)
cfg.CONF.register_opts(zookeeper_opts, zookeeper_group)

kafka_common_opts = [cfg.StrOpt('uri'),
                     cfg.StrOpt('group_id'),
                     cfg.StrOpt('topic'),
                     cfg.StrOpt('consumer_id'),
                     cfg.StrOpt('client_id'),
                     cfg.IntOpt('database_batch_size'),
                     cfg.IntOpt('max_wait_time_seconds'),
                     cfg.IntOpt('fetch_size_bytes'),
                     cfg.IntOpt('buffer_size'),
                     cfg.IntOpt('max_buffer_size'),
                     cfg.StrOpt('zookeeper_path')]

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

cassandra_opts = [cfg.StrOpt('cluster_ip_addresses'),
                  cfg.StrOpt('keyspace')]

cassandra_group = cfg.OptGroup(name='cassandra')
cfg.CONF.register_group(cassandra_group)
cfg.CONF.register_opts(cassandra_opts, cassandra_group)

cfg.CONF(sys.argv[1:], project='monasca', prog='persister')
log.setup(cfg.CONF, "monasca-persister")


def main():
    """Start persister.

    Start metric persister and alarm persister in separate threads.
    """

    database_type = cfg.CONF.database.database_type

    if database_type is None:
        LOG.warn("Database type is not configured.")
        LOG.warn("Using influxdb for default database type.")
        LOG.warn("Please configure a database type using the 'database_type' "
                 "property in the config file.")

    # Allow None for database_type for backwards compatibility.
    if database_type is None or database_type.lower() == 'influxdb':

        metric_persister = MetricInfluxdbPersister(cfg.CONF.kafka_metrics,
                                                   cfg.CONF.influxdb,
                                                   cfg.CONF.zookeeper)

        alarm_persister = AlarmStateHistInfluxdbPersister(
                cfg.CONF.kafka_alarm_history,
                cfg.CONF.influxdb,
                cfg.CONF.zookeeper)

    elif database_type.lower() == 'cassandra':

        metric_persister = MetricCassandraPersister(
                cfg.CONF.kafka_metrics,
                cfg.CONF.cassandra,
                cfg.CONF.zookeeper)

        alarm_persister = AlarmStateHistCassandraPersister(
                cfg.CONF.kafka_alarm_history,
                cfg.CONF.cassandra,
                cfg.CONF.zookeeper)

    else:

        LOG.error("Unknown database type [{}] is not implemented".format(
                database_type))
        LOG.error("Known database types are [influxdb] and [cassandra]")
        LOG.error("Please configure a known database type in the config file.")
        os._exit(1)

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
    DB but does not reach the commit.
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

    def __init__(self, kafka_conf, db_conf, zookeeper_conf):

        super(AbstractPersister, self).__init__()

        self._data_points = []

        self._kafka_topic = kafka_conf.topic

        self._database_batch_size = kafka_conf.database_batch_size

        self._consumer = KafkaConsumer(
                kafka_conf.uri,
                zookeeper_conf.uri,
                kafka_conf.zookeeper_path,
                kafka_conf.group_id,
                kafka_conf.topic,
                repartition_callback=self._flush,
                commit_callback=self._flush,
                commit_timeout=kafka_conf.max_wait_time_seconds)

        self.init_db(db_conf)

    @abc.abstractmethod
    def init_db(self, db_conf):
        pass

    @abc.abstractmethod
    def process_message(self, message):
        pass

    @abc.abstractmethod
    def execute_batch(self, data_points):
        pass

    def _flush(self):
        if not self._data_points:
            return

        try:
            self.execute_batch(self._data_points)

            LOG.info("Processed {} messages from topic '{}'".format(
                    len(self._data_points), self._kafka_topic))

            self._data_points = []
            self._consumer.commit()
        except Exception:
            LOG.exception("Error writing to database: {}"
                          .format(self._data_points))
            raise

    def run(self):
        try:
            for raw_message in self._consumer:
                try:
                    message = raw_message[1]
                    data_point = self.process_message(message)
                    self._data_points.append(data_point)
                except Exception:
                    LOG.exception('Error processing message. Message is '
                                  'being dropped. {}'.format(message))

                if len(self._data_points) >= self._database_batch_size:
                    self._flush()
        except:
            LOG.exception(
                    'Persister encountered fatal exception processing '
                    'messages. '
                    'Shutting down all threads and exiting')
            shutdown_all_threads_and_die()


@six.add_metaclass(abc.ABCMeta)
class AbstractCassandraPersister(AbstractPersister):

    def __init__(self, kafka_conf, cassandra_db_conf, zookeeper_conf):

        super(AbstractCassandraPersister, self).__init__(
                kafka_conf, cassandra_db_conf, zookeeper_conf)

    def init_db(self, cassandra_db_conf):

        self._cassandra_cluster = Cluster(
                cassandra_db_conf.cluster_ip_addresses.split(','))

        self.cassandra_session = self._cassandra_cluster.connect(
                cassandra_db_conf.keyspace)

        self._batch_stmt = BatchStatement()

class MetricMeasurementInfo(object):

    def __init__(self, tenant_id, region, metric_hash, metric_set,
                 measurement):

        self.tenant_id = tenant_id
        self.region = region
        self.metric_hash = metric_hash
        self.metric_set = metric_set
        self.measurement = measurement


class MetricCassandraPersister(AbstractCassandraPersister):

    def __init__(self, kafka_conf, cassandra_db_conf, zookeeper_conf):

        super(MetricCassandraPersister, self).__init__(
            kafka_conf,
            cassandra_db_conf,
            zookeeper_conf)

        self._insert_measurement_stmt = self.cassandra_session.prepare(
                'insert into measurements (tenant_id,'
                'region, metric_hash, time_stamp, value,'
                'value_meta) values (?, ?, ?, ?, ?, ?)')

        self._insert_metric_map_stmt = self.cassandra_session.prepare(
                'insert into metric_map (tenant_id,'
                'region, metric_hash, '
                'metric_set) values'
                '(?,?,?,?)')

    def process_message(self, message):

        (dimensions, metric_name, region, tenant_id, time_stamp, value,
         value_meta) = parse_measurement_message(message)

        metric_hash, metric_set = create_metric_hash(metric_name,
                                                     dimensions)

        measurement = (tenant_id.encode('utf8'),
                       region.encode('utf8'),
                       metric_hash,
                       time_stamp,
                       value,
                       json.dumps(value_meta, ensure_ascii=False).encode(
                           'utf8'))

        LOG.debug(measurement)

        return MetricMeasurementInfo(
                tenant_id.encode('utf8'),
                region.encode('utf8'),
                metric_hash,
                metric_set,
                measurement)

    def execute_batch(self, metric_measurement_infos):

        for metric_measurement_info in metric_measurement_infos:

            self._batch_stmt.add(self._insert_measurement_stmt,
                                 metric_measurement_info.measurement)

            metric_map = (metric_measurement_info.tenant_id,
                          metric_measurement_info.region,
                          metric_measurement_info.metric_hash,
                          metric_measurement_info.metric_set)

            self._batch_stmt.add(self._insert_metric_map_stmt,
                                 metric_map)

        self.cassandra_session.execute(self._batch_stmt)

        self._batch_stmt = BatchStatement()

def create_metric_hash(metric_name, dimensions):

    metric_name_part = '__name__' + '=' + urllib.quote_plus(metric_name)

    hash_string = metric_name_part

    metric_set = set()

    metric_set.add(metric_name_part)

    for dim_name in sorted(dimensions.iterkeys()):
        dimension = (urllib.quote_plus(dim_name) + '=' + urllib.quote_plus(
                dimensions[dim_name]))
        metric_set.add(dimension)
        hash_string += dimension

    sha1_hash = hashlib.sha1(hash_string).hexdigest()

    return bytearray.fromhex(sha1_hash), metric_set


class AlarmStateHistCassandraPersister(AbstractCassandraPersister):

    def __init__(self, kafka_conf, cassandra_db_conf, zookeeper_conf):

        super(AlarmStateHistCassandraPersister, self).__init__(
                kafka_conf,
                cassandra_db_conf,
                zookeeper_conf)

        self._insert_alarm_state_hist_stmt = self.cassandra_session.prepare(
                'insert into alarm_state_history (tenant_id, alarm_id, '
                'metrics, new_state, '
                'old_state, reason, reason_data, '
                'sub_alarms, time_stamp) values (?,?,?,?,?,?,?,?,?)')

    def process_message(self, message):

        (alarm_id, metrics, new_state, old_state, state_change_reason,
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

    def execute_batch(self, alarm_state_hists):

        for alarm_state_hist in alarm_state_hists:
            self._batch_stmt.add(self._insert_alarm_state_hist_stmt,
                                 alarm_state_hist)

        self.cassandra_session.execute(self._batch_stmt)

        self._batch_stmt = BatchStatement()


@six.add_metaclass(abc.ABCMeta)
class AbstractInfluxdbPersister(AbstractPersister):

    def __init__(self, kafka_conf, influxdb_db_conf, zookeeper_conf):

        super(AbstractInfluxdbPersister, self).__init__(
            kafka_conf, influxdb_db_conf, zookeeper_conf)

    def init_db(self, influxdb_db_conf):

        self._influxdb_client = InfluxDBClient(influxdb_db_conf.ip_address,
                                               influxdb_db_conf.port,
                                               influxdb_db_conf.user,
                                               influxdb_db_conf.password,
                                               influxdb_db_conf.database_name)

    def execute_batch(self, data_points):

        self._influxdb_client.write_points(data_points, 'ms')


class AlarmStateHistInfluxdbPersister(AbstractInfluxdbPersister):

    def __init__(self, kafka_conf, influxdb_db_conf, zookeeper_conf):

        super(AlarmStateHistInfluxdbPersister, self).__init__(
                kafka_conf, influxdb_db_conf, zookeeper_conf)

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


class MetricInfluxdbPersister(AbstractInfluxdbPersister):

    def __init__(self, kafka_conf, influxdb_db_conf, zookeeper_conf):

        super(MetricInfluxdbPersister, self).__init__(kafka_conf,
                                                      influxdb_db_conf,
                                                      zookeeper_conf)

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


def parse_measurement_message(message):

    LOG.debug(message.message.value.decode('utf8'))

    decoded_message = json.loads(message.message.value)
    LOG.debug(json.dumps(decoded_message, sort_keys=True, indent=4))

    metric = decoded_message['metric']

    metric_name = metric['name']
    LOG.debug('name: %s', metric_name)

    creation_time = decoded_message['creation_time']
    LOG.debug('creation time: %s', creation_time)

    region = decoded_message['meta']['region']
    LOG.debug('region: %s', region)

    tenant_id = decoded_message['meta']['tenantId']
    LOG.debug('tenant id: %s', tenant_id)

    dimensions = {}
    if 'dimensions' in metric:
        for dimension_name in metric['dimensions']:
            dimensions[dimension_name.encode('utf8')] = (
                metric['dimensions'][dimension_name].encode('utf8'))
            LOG.debug('dimension: %s : %s', dimension_name,
                      dimensions[dimension_name])

    time_stamp = metric['timestamp']
    LOG.debug('timestamp %s', time_stamp)

    value = float(metric['value'])
    LOG.debug('value: %s', value)

    if 'value_meta' in metric and metric['value_meta']:

        value_meta = metric['value_meta']

    else:

        value_meta = {}
    LOG.debug('value_meta: %s', value_meta)

    return (dimensions, metric_name, region, tenant_id, time_stamp, value,
            value_meta)


def parse_alarm_state_hist_message(message):

    LOG.debug(message.message.value.decode('utf8'))

    decoded_message = json.loads(message.message.value)
    LOG.debug(json.dumps(decoded_message, sort_keys=True, indent=4))

    alarm_transitioned = decoded_message['alarm-transitioned']

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

    link = alarm_transitioned['link'] or ""
    LOG.debug('link: %s', link)

    lifecycle_state = alarm_transitioned['lifecycleState'] or ""
    LOG.debug('lifecycle_state: %s', lifecycle_state)

    state_change_reason = alarm_transitioned[
        'stateChangeReason']
    LOG.debug('state change reason: %s', state_change_reason)

    tenant_id = alarm_transitioned['tenantId']
    LOG.debug('tenant id: %s', tenant_id)

    time_stamp = alarm_transitioned['timestamp']
    LOG.debug('time stamp: %s', time_stamp)

    sub_alarms = alarm_transitioned['subAlarms']
    if sub_alarms:

        sub_alarms_json = json.dumps(sub_alarms, ensure_ascii=False)

        sub_alarms_json_snake_case = sub_alarms_json.replace(
                '"subAlarmExpression":',
                '"sub_alarm_expression":')

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
