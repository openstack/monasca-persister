# (C) Copyright 2019 StackHPC Limited.
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

""" Monasca InfluxDB tool for migration to database per tenant

    Used to move data from monolithic database e.g. `monasca` to a database per
    tenant model, e.g. `monasca_<tenant_id>`.

    Please see the included README.rst for more details about creating an
    appropriate configuration file.
"""

import os
import re
import sys

from monasca_persister import config
from monasca_persister.repositories.influxdb import metrics_repository

from oslo_config import cfg

from oslo_log import log

LOG = log.getLogger(__name__)

MIGRATE_QUERY = ('SELECT * INTO "{target_db}"..:MEASUREMENT'
                 ' FROM "{measurement}"'
                 ' WHERE _tenant_id=\'{tenant_id}\''
                 ' AND time > {lower_time_offset}'
                 ' AND time <= {upper_time_offset}'
                 ' GROUP BY *')

class MigrationHelper(object):

    def __init__(self):
        repo = metrics_repository.MetricInfluxdbRepository()
        self.conf = repo.conf
        self.client = repo._influxdb_client
        self.client.switch_database(self.conf.influxdb.database_name)

    def _migrate(self, measurement, tenant_id, start_time_offset,
                 end_time_offset, retention_policy={}, time_unit='w',
                 db_per_tenant=True, **kwargs):

        total_written = 0
        first_upper_time_offset = None
        last_lower_time_offset = None
        time_offset = start_time_offset

        if db_per_tenant:
            target_db = "{}_{}".format(self.conf.influxdb.database_name, tenant_id)
        self.client.create_database(target_db)
        if retention_policy:
            self.client.create_retention_policy(database=target_db, **retention_policy)
        LOG.info('         into {}:'.format(target_db))

        while end_time_offset > 0 and time_offset < end_time_offset:
            lower_time_offset = 'now()-{}{}'.format(time_offset + 1, time_unit)
            upper_time_offset = 'now()-{}{}'.format(time_offset, time_unit)
            if not first_upper_time_offset:
                first_upper_time_offset = upper_time_offset
            migrate_query = MIGRATE_QUERY.format(
                target_db=target_db,
                measurement=measurement,
                tenant_id=tenant_id,
                lower_time_offset=lower_time_offset,
                upper_time_offset=upper_time_offset,
            )
            LOG.debug(migrate_query)

            written = next(self.client.query(migrate_query).get_points('result')).get('written')
            total_written += written
            time_offset += 1
            if written > 0:
                last_lower_time_offset = lower_time_offset
                LOG.info("         migrated {} entries from {} -> {} (cumulative {})".format(
                    written,
                    lower_time_offset,
                    upper_time_offset,
                    total_written,
                ))
        LOG.info("         finished migrating a total of {} entries from {} -> {}.".format(
            total_written,
            last_lower_time_offset,
            first_upper_time_offset,
        ))

    def get_measurements(self, fname):
        measurements = []
        if fname:
            with open(fname, 'a+') as f:
                measurements = [line.strip() for line in f.readlines()]
        if not measurements:
            result = self.client.query('SHOW MEASUREMENTS').get_points('measurements')
            measurements = [m.get('name') for m in result]
            if fname:
                with open(fname, 'w') as f:
                    for r in measurements:
                        f.write(r + '\n')
        return measurements

    def get_tenancies(self, measurements):
        result = self.client.query("SHOW TAG VALUES WITH KEY = _tenant_id")
        return {m: [t.get('value') for t in result.get_points(m)] for m in measurements}

    def get_complete(self, fname):
        if fname:
            with open(fname, 'a+') as fd:
                return {line.strip() for line in fd.readlines()}
        else:
            return {}

    def migrate(self,
                tenant_defaults={},
                default_start_time_offset=0,  # Default: now
                default_end_time_offset=520,  # Default: 10 years
                skip_regex=[],
                measurements_file=None, success_file=None, failure_file=None, **kwargs):
        measurements = self.get_measurements(measurements_file)
        tenancy = self.get_tenancies(measurements)
        done = self.get_complete(success_file)
        default_rp = {}
        hours = self.conf.influxdb.default_retention_hours
        if hours > 0:
            rp = '{}h'.format(hours)
            default_rp = dict(name=rp, duration=rp, replication='1', default=True)
        skip = set()
        fail = set()
        if failure_file:
            if os.path.exists(failure_file):
                os.remove(failure_file)

        filtered_measurements = []
        for measurement in measurements:
            if any([f.match(measurement) for f in skip_regex]):
                skip.add(measurement)
                LOG.debug('Skipping {} because it matches a skip regex.'.format(measurement))
                continue
            elif measurement in done:
                LOG.debug('Skipping {} because its already done.'.format(measurement))
                continue
            else:
                filtered_measurements.append(measurement)

        for i, measurement in enumerate(filtered_measurements):
            LOG.info('Migrating {}'.format(measurement))
            try:
                for tenant_id in tenancy.get(measurement):
                    start_time_offset = tenant_defaults.get(
                        tenant_id, {}).get('start_time_offset_override',
                                           default_start_time_offset)
                    end_time_offset = tenant_defaults.get(
                        tenant_id, {}).get('end_time_offset_override',
                                           default_end_time_offset)
                    # NOTE (brtknr): Ensure that the default upper and lower
                    # time offsets are respected during migration by the
                    # projects with custom retention policies.
                    start_time_offset = max(default_start_time_offset,
                                            start_time_offset)
                    end_time_offset = min(default_end_time_offset,
                                          end_time_offset)
                    retention_policy = tenant_defaults.get(
                        tenant_id, {}).get('rp', default_rp)
                    self._migrate(measurement, tenant_id,
                                  start_time_offset=start_time_offset,
                                  end_time_offset=end_time_offset,
                                  retention_policy=retention_policy, **kwargs)
                if success_file:
                    with open(success_file, 'a+') as fd:
                        fd.write('{}\n'.format(measurement))
                done.add(measurement)
            except Exception as e:
                LOG.error(e)
                if failure_file:
                    with open(failure_file, 'a+') as fe:
                        fe.write('{}\t{}\n'.format(measurement, e))
                fail.add(measurement)
            LOG.info("{}/{} (done {} + skip {} + fail {})/{}".format(
                i + 1, len(filtered_measurements), len(done), len(skip),
                len(fail), len(measurements)))


def main():
    CONF = cfg.CONF
    cli_opts = [
        cfg.StrOpt('migrate-time-unit', choices=['h', 'd', 'w'], default='w',
                   help='Unit of time, h=hour, d=day, w=week (default: "w").'),
        cfg.IntOpt('migrate-start-time-offset', default=0,
                   help='Start time offset in the given unit of time (default: 0).'),
        cfg.IntOpt('migrate-end-time-offset', default=520,
                   help='End time offset in the given unit of time (default: 520).'),
        cfg.DictOpt('migrate-retention-policy', default={},
                    help=('Custom retention policy for projects in the provided'
                          'time unit. (e.g. project-id-x:2,project-id-y:4)')),
        cfg.ListOpt('migrate-skip-regex', default=[],
                    help=('Skip metrics that match this comma separated list of regex patterns.'
                          '(e.g. ^log\\\\..+,^cpu\\\\..+ to skip metrics beginning with log.)')),
    ]
    CONF.register_cli_opts(cli_opts)
    config.parse_args("Monasca InfluxDB database per tenant migration tool")

    # Configure custom retention policy for your existing projects. For
    # example, rp2w is a retention policy of two weeks which we can assign to
    # project example-project-id.
    tenant_defaults = dict()
    for k, v in CONF.migrate_retention_policy.items():
        if v.isdigit():
            rp = '{}{}'.format(v, CONF.migrate_time_unit)
            tenant_defaults[k] = dict(
                end_time_offset_override=int(v),
                rp=dict(name=rp, duration=rp, replication='1', default=True),
            )
            LOG.info('Project {} will be applied retention policy: {}.'.format(k, rp))
        else:
            raise ValueError('Retention policy for project {} must be an'
                             'integer of given time unit. Current value:'
                             '{}.'.format(k, v))

    skip_regex = []
    for p in CONF.migrate_skip_regex:
        skip_regex.append(re.compile(str(p)))
        LOG.info('Metrics matching pattern "{}" will be skipped.'.format(p))

    helper = MigrationHelper()
    helper.migrate(skip_regex=skip_regex,
                   tenant_defaults=tenant_defaults,
                   default_end_time_offset=CONF.migrate_end_time_offset,
                   default_start_time_offset=CONF.migrate_start_time_offset,
                   time_unit=CONF.migrate_time_unit,
                   measurements_file='migrate-measurements',
                   success_file='migrate-success',
                   failure_file='migrate-failure')
    return 0


if __name__ == "__main__":
    sys.exit(main())
