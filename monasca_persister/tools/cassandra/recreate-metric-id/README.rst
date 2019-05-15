persister-recreate-metric-id
============================

In some rare cases, it is possible to have metric rows in the Cassandra
database which do not have a metric_id.  Due to the nature of TSDBs,
it is valid to have sparse data, but the version of Monasca API up
through Rocky do not handle this well and produce an ugly ERROR.

For further reading - https://storyboard.openstack.org/#!/story/2005305

This tool runs through the metric table in Cassandra, identifies rows
that are missing a metric_id, and uses an UPDATE operation to recreate
the metric_id based on other values.  The metric_id is calculated from
a hash of the region, tenant_id, metric_name and dimensions, so it can
be recreated.

All effort has been made to ensure this is a safe process. And it
should be safe to run the tool multiple times.  However, it is provided
AS IS and you should use it at your own risk.

Usage
=====

Steps to use this tool:

- Log in to one node where monasca-persister is deployed.
- Identify installation path to monasca-persister.  This may be a
  virtual environment such as
  `/opt/stack/venv/monasca-<version>/lib/python2.7/site-packages/monasca-persister`
  or as in devstack `/opt/stack/monasca-persister/monasca_persister/`.
- Identify the existing configuration for monasca-persister. If using a
  java deployment, it may be in `/opt/stack/service/monasca/etc/persister-config.yml`
  or in devstack `/etc/monasca/persister.conf`
- Copy and modify the config template file.

::

   cp persister-recreate.ini /opt/stack/service/monasca/etc/persister-recreate.ini
   vi /opt/stack/service/monasca/etc/persister-recreate.ini

- Copy the values from the monasca-persister config in to the new .ini,
  particularly the password.  In some cases, the single IP for the
  management network of one of the Cassandra nodes may need to be given,
  rather than the list of hostnames as specified in the .yml.
- Copy the `persister-recreate-metric-id.py` and `persister-check-missing-metric-id.py`
  files in to place with the monasca-persister code.

::

   cp persister-*-metric-id.py /opt/stack/venv/monasca-<version>/lib/python2.7/site-packages/monasca-persister

- Ensure the `mon-persister` user has permission to access both
  `persister-recreate.ini` and `persister-recreate-metric-id.py`.
- Invoke the tool to generate a log of rows needing repair.

::

   sudo -u mon-persister /opt/stack/venv/monasca-<version>/bin/python /opt/stack/venv/monasca-<version>/lib/python2.7/site-packages/monasca_persister/persister-check-missing-metric-id.py --config-file /opt/stack/service/monasca/etc/persister-recreate.ini

- Review the logged output.  If output is as expected, then invoke
  the recreate-missing-metric-id tool to repair the rows.

::

   sudo -u mon-persister /opt/stack/venv/monasca-<version>/bin/python /opt/stack/venv/monasca-<version>/lib/python2.7/site-packages/monasca_persister/persister-recreate-metric-id.py  --config-file /opt/stack/service/monasca/etc/persister-recreate.ini

- Once repair has been verified successful, the configuration file
  may be deleted.


License
=======

Copyright (c) 2019 SUSE LLC

Licensed under the Apache License, Version 2.0 (the “License”); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

::

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an “AS IS” BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.