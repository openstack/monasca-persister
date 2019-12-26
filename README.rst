Monasca Persister
=================

.. image:: https://governance.openstack.org/tc/badges/monasca-persister.svg
    :target: https://governance.openstack.org/tc/reference/tags/index.html

.. Change things from this point on

The Monasca Persister consumes metrics and alarm state transitions
from the Apache Kafka message queue and stores them in the time series
database.


Running
=======

To install the Python monasca-persister modules, git clone the source
and run the following command:

::

   $ pip install -c https://releases.openstack.org/constraints/upper/master -e ./monasca-persister

To run the unit tests use:

::

   $ tox -e py36

To start the persister run:

::

   $ monasca-persister --config-file=monasca-persister.conf


Configuration
=============

A sample configuration file can be generated using the Oslo standards
used in other OpenStack projects.

::

   tox -e genconfig

The result will be in ./etc/monasca/monasca-persister.conf.sample

If the deployment is using the Docker files, the configuration template
can be found in docker/monasca-persister.conf.j2.


Java
====

For information on Java implementation see `java/Readme.rst <java/Readme.rst>`_.


Contributing and Reporting Bugs
===============================

Ongoing work for the Monasca project is tracked in Storyboard_.


License
=======

Copyright (c) 2014 Hewlett-Packard Development Company, L.P.

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

::

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


.. _Storyboard: https://storyboard.openstack.org
