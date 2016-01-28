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
   in into Influxdb

   Start the perister as stand-alone process by running 'persister.py
   --config-file <config file>'
"""

import sys

import simport
from oslo_config import cfg
from oslo_log import log

from repositories.persister import Persister

LOG = log.getLogger(__name__)

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

repositories_opts = [
    cfg.StrOpt('metrics_driver', help='The repository driver to use for metrics'),
    cfg.StrOpt('alarm_state_history_driver', help='The repository driver to use for alarm state history')]

repositories_group = cfg.OptGroup(name='repositories', title='repositories')
cfg.CONF.register_group(repositories_group)
cfg.CONF.register_opts(repositories_opts, repositories_group)


def main():
    log.register_options(cfg.CONF)
    log.set_defaults()
    cfg.CONF(sys.argv[1:], project='monasca', prog='persister')
    log.setup(cfg.CONF, "monasca-persister")

    """Start persister.

    Start metric persister and alarm persister in separate threads.
    """

    metric_repository = simport.load(cfg.CONF.repositories.metrics_driver)
    alarm_state_history_repository = simport.load(cfg.CONF.repositories.alarm_state_history_driver)

    metric_persister = Persister(cfg.CONF.kafka_metrics,
                                 cfg.CONF.zookeeper,
                                 metric_repository)

    alarm_persister = Persister(cfg.CONF.kafka_alarm_history,
                                cfg.CONF.zookeeper,
                                alarm_state_history_repository)

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

if __name__ == "__main__":
    sys.exit(main())
