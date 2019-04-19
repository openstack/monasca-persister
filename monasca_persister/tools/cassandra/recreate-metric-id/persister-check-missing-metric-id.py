# (C) Copyright 2019 SUSE LLC
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

"""Persister check for missing metric_id tool

   This tool is designed to 'fix' the rare instance when a metric_id
   has been removed from a row in Cassandra. That can cause issues
   when monasca-api retrieves the metric and tries to decode it.

   Configure this tool by copying the Monasca Persister settings from
   /opt/stack/service/monasca/etc/persister-config.yml in to a config
   .ini file (see template).

   Start the tool as stand-alone process by running
   'sudo -u mon_persister <venv python> \
   <venv path>/site-packages/monasca_persister/persister-check-missing-metric-id.py \
   --config-file <config file>'

   When done, you may delete the config .ini file.

   Template for .ini file (suggested /opt/stack/service/monasca/etc/persister-recreate.ini)
   [DEFAULT]
   debug = False

   [repositories]
   metrics_driver = monasca_persister.repositories.cassandra.metrics_repository: \
   MetricCassandraRepository

   [cassandra]

   # Comma separated list of Cassandra node IP addresses (list value)
   contact_points = <single ip address for mgmt network on this node>

   # Cassandra port number (integer value)
   port = 9042

   # Keyspace name where metrics are stored (string value)
   #keyspace = monasca

   # Cassandra user name (string value)
   user = mon_persister

   # Cassandra password (string value)
   password = <password from persister-config.yml>

"""
import sys

from oslo_log import log

from monasca_persister import config

from monasca_persister.repositories.cassandra import connection_util

LOG = log.getLogger(__name__)

METRIC_ALL_CQL = ('select region, tenant_id, metric_name, dimensions, '
                  'dimension_names, created_at, metric_id, updated_at '
                  'from metrics')


def usage():
    usage = """Monasca Persister Check for Missing metric_id Tool

    Used to find a metric_id, which in rare cases may be deleted
    from a row.  The metric_id is a hash of other fields, and thus can be
    recreated.  Note this tool is only for use with Cassandra storage installs.

    Please see the included README.rst for more details about creating an
    appropriate configuration file.

    To get a report of what rows are missing values (execute as mon-persister user):
    persister-recreate-metric-id.py --config-file <path>/persister-recreate.ini

    """
    print(usage)


def main():
    """persister check for missing metric_id tool."""

    config.parse_args()

    try:
        LOG.info('Starting check of metric_id consistency.')

        # Connection setup
        # rocky style - note that we don't deliver pike style
        _cluster = connection_util.create_cluster()
        _session = connection_util.create_session(_cluster)

        metric_all_stmt = _session.prepare(METRIC_ALL_CQL)

        rows = _session.execute(metric_all_stmt)

        # if rows:
        #     LOG.info('First - {}'.format(rows[0]))
        #     # LOG.info('First name {} and id {}'.format(
        #     #     rows[0].metric_name, rows[0].metric_id)) # metric_id can't be logged raw

        # Bit of a misnomer - "null" is not in the cassandra db
        missing_value_rows = []
        for row in rows:
            if row.metric_id is None:
                LOG.info('Row with missing metric_id - {}'.format(row))
                missing_value_rows.append(row)

                # check created_at
                if row.created_at is None and row.updated_at is not None:
                    LOG.info("Metric created_at was also None.")

                # TODO(joadavis) update the updated_at timestamp to now

                # recreate metric id
                # copied from metrics_repository.py
                hash_string = '%s\0%s\0%s\0%s' % (row.region, row.tenant_id,
                                                  row.metric_name,
                                                  '\0'.join(row.dimensions))
                # metric_id = hashlib.sha1(hash_string.encode('utf8')).hexdigest()
                # id_bytes = bytearray.fromhex(metric_id)

                LOG.info("Recreated hash for metric id: {}".format(hash_string))
                # LOG.info("new id_bytes {}".format(id_bytes)) # can't unicode decode for logging

        # LOG.info("of {} rows there are {} missing metric_id".format(len(rows), len(null_rows)))
        if len(missing_value_rows) > 0:
            LOG.warning("--> There were {} rows missing metric_id.".format(
                len(missing_value_rows)))
            LOG.warning("    Those rows have NOT been updated.\n"
                        "    Please run the persister-recreate-metric-id "
                        "tool to repair the rows.")
        else:
            LOG.info("No missing metric_ids were found, no changes made.")

        LOG.info('Done with metric_id consistency check.')

        return 0

    except Exception:
        LOG.exception('Error! Exiting.')


if __name__ == "__main__":
    sys.exit(main())
