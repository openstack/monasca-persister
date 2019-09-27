migrate-to-db-per-tenant.py
===========================

The general plan for the monasca project is to move in the direction of
having a database per tenant because:
- Not only give a finer grain control over retention policy per tenant
  but also possibly speed up tenants queries by scoping them within their
  project.
- Security is improved though better isolation. For example, a previous bug in
  InfluxDB where the tenant ID was ignored in the query exposed data from
  outside a tenants project. This is less likely to happen with a separate DB
  per tenant.)
- We move in a direction of improving scalability for InfluxDB users
  without the Enterprise license. In the future a dedicated InfluxDB
  instance could optionally be used per project.

For further reading - https://storyboard.openstack.org/#!/story/2006331

All effort has been made to ensure this is a safe process. And it
should be safe to run the tool multiple times.  However, it is provided
AS IS and you should use it at your own risk.

Usage
=====

Steps to use this tool:

- Log in to one node where monasca-persister is deployed.

- Identify installation path to monasca-persister. This may be a
  virtual environment such as
  `/opt/stack/venv/monasca-<version>/lib/python2.7/site-packages/monasca_persister`
  or as in devstack
  `/opt/stack/monasca-persister/monasca_persister/` which you may need to
  activate.

- Identify the existing configuration for monasca-persister. The likely
  location for this is `/etc/monasca/persister.conf` if using devstack.

- Optionally, make a backup of your database in case something goes
  wrong but your original database should be left intact.

- Open and edit `migrate-to-db-per-tenant.py` and edit mapping between
  your existing projects to retention policies if you so wish. You may
  also want to change `end_time_offset_override` to the length of history
  you want the migration tool to consider.

- Invoke the tool to migrate to database per tenant. The arguments inside the
  square bracket default to the values shown when undefined.

::

   sudo -u mon-persister python migrate-to-db-per-tenant.py --config-file /etc/monasca/persister.conf --migrate-retention-policy project:2w,project2:1d --migrate-skip-regex ^log\\..+ [--migrate-time-unit w --migrate-start-time-offset 0 --migrate-end-time-offset 520]


- The progress of the migration will be logged to persister log file
  specifed in the persister config.


FAQ
===

1. Will this interrupt the operation of Monasca?
-  In theory, you can run this migration query on a live Monasca system
   because the migration leaves the original database untouched.
   However, in practice, it may degrade performance while the migration
   is taking place.
2. How do I ensure that I migrate *all* metrics to the new scheme? i.e.
   Do I need to stop all persisters writing to InfluxDB and let Kafka
   buffer for a bit while the migration is taking place?
-  If you stop the persister and the migration takes longer than the
   Kafka buffer duration, this may cause data loss. It may be best to do
   the first batch of migration on the entire dataset and perform
   iterative migration of smaller chucks collected in the duration
   migration was taking place.
3. Will the original index be left intact so that I can fall back if it
   doesn't work?
-  Yes.
4. What do I do after I have run the tool? I need to enable the feature
   before starting the persisters right?
-  If you enable the feature by flipping on `db_per_tenant` flag before
   starting the migration and restart your persister service, your
   persister will start writing to a database per tenant model. It may
   be best to switch this feature on after the first round of migration,
   restart the persister so that it starts writing to database per
   tenancy and migrate the remainder of the data. Note that in the
   second run, you may need to delete `migrate-success` so that the metrics
   are not skipped. You may also want to try with smaller `--migrate-time-unit`
   option (default: `w` for week) if larger chunks of migration fail. Other
   option are `d` for day and `h` for hours.
5. How long does it take (rough estimate per GB of metrics?)
-  This depends on your storage backend. On one of the systems in which
   this tool was tested on, it was an overnight job to move 40GB of metrics.
6. Is the tool idempotent? i.e. could I migrate the bulk of the data
   with the persisters running, stop the persisters, and then migrate
   the delta in a short interval to minimise downtime? Can I run it
   again if it fails?
-  Yes, InfluxDB ensures that same time indices with the same tags,
   fields and values are not duplicated. Copying things over twice is
   perfectly safe.
7. Will it set fire to my cat?
-  Depends if you cat is well behaved.


License
=======

Copyright (c) 2019 StackHPC Limited

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
