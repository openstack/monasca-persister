monasca-persister
=================

.. warning::

    Java implementation of monasca-persister is deprecated as of Train release.

The Monasca Persister consumes metrics and alarm state transitions
from the Apache Kafka message queue and stores them in the time series
database.

Although the Persister isn't primarily a Web service it uses DropWizard,
https://dropwizard.github.io/dropwizard/, which provides a nice Web
application framework to expose an http endpoint that provides an
interface through which metrics about the Persister can be queried as
well as health status.

The basic design of the Persister is to have one Kafka consumer publish
to a Disruptor, https://github.com/LMAX-Exchange/disruptor, that has
output processors. The output processors use prepared batch statements
to write to the Metrics and Alarms database.

The number of output processors/threads in the Persister can be
specified to scale to more messages. To horizontally scale and provide
fault-tolerance any number of Persisters can be started as consumers
from the Message Queue.

Build
=====

Requires monasca-common from
https://opendev.org/openstack/monasca-common. Download and build
following instructions in its README.rst. Then build monasca-persister
by:

::

   mvn clean package

Configuration
=============

A sample configuration file is available in
java/src/deb/etc/persister-config.yml-sample.

A second configuration file is provided in
java/src/main/resources/persister-config.yml for use with the `vagrant
"mini-mon" development environment`_.

TODO
====

The following list is historic. Current work is tracked in `Storyboard`_.

-  Purge metrics on shutdown
-  Add more robust offset management in Kafka. Currently, the offset is
   advanced as each message is read. If the Persister stops after the
   metric has been read and prior to it being committed to the Metrics
   and Alarms database, the metric will be lost.
-  Add better handling of SQL exceptions.
-  Complete health check.
-  Specify and document the names of the metrics that are available for
   monitoring of the Persister.
-  Document the yaml configuration parameters.

.. _vagrant "mini-mon" development environment: https://github.com/openstack/monasca-vagrant/
.. _Storyboard: https://storyboard.openstack.org

