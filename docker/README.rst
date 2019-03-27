==================================
Docker image for Monasca persister
==================================
The Monasca persister image is based on the monasca-base image.


Building monasca-base image
===========================
See https://github.com/openstack/monasca-common/tree/master/docker/README.rst


Building Monasca persister image (child)
========================================


Requirements from monasca-base image
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
health_check.py
  This file will be used for checking the status of the Monasca persister
  application.


Scripts for child image
~~~~~~~~~~~~~~~~~~~~~~~
start.sh
  In this starting script provide all steps that lead to the proper service
  start. Including usage of wait scripts and templating of configuration
  files. You also could provide the ability to allow running container after
  service died for easier debugging.

build_image.sh
  Please read detailed build description inside the script.


Build arguments (child)
~~~~~~~~~~~~~~~~~~~~~~~
====================== =========================
Arguments              Occurrence
====================== =========================
BASE_TAG               Dockerfile
====================== =========================


Environment variables (child)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
=============================== =============================== ================================================
Variable                        Default                         Description
=============================== =============================== ================================================
DEBUG                           false                           If true, enable debug logging
VERBOSE                         true                            If true, enable info logging
ZOOKEEPER_URI                   zookeeper:2181                  The host and port for zookeeper
KAFKA_URI                       kafka:9092                      The host and port for kafka
KAFKA_WAIT_FOR_TOPICS           alarm-state-transitions,metrics Topics to wait on at startup
KAFKA_WAIT_RETRIES 	            24                              Number of kafka connect attempts
KAFKA_WAIT_DELAY                5                               Seconds to wait between attempts
KAFKA_ALARM_HISTORY_BATCH_SIZE  1000                            Kafka consumer takes messages in a batch
KAFKA_ALARM_HISTORY_WAIT_TIME   15                              Seconds to wait if the batch size is not reached
KAFKA_METRICS_BATCH_SIZE        1000                            Kafka consumer takes messages in a batch
KAFKA_METRICS_WAIT_TIME         15                              Seconds to wait if the batch size is not reached
DATABASE_BACKEND                influxdb                        Select for backend database
INFLUX_HOST                     influxdb                        The host for influxdb
INFLUX_PORT                     8086                            The port for influxdb
INFLUX_USER                     mon_persister                   The influx username
INFLUX_PASSWORD                 password                        The influx password
INFLUX_DB                       mon                             The influx database name
INFLUX_IGNORE_PARSE_POINT_ERROR false                           Don't exit on InfluxDB parse point errors
CASSANDRA_HOSTS                 cassandra                       Cassandra node addresses
CASSANDRA_PORT                  8086                            Cassandra port number
CASSANDRA_USER                  mon_persister                   Cassandra user name
CASSANDRA_PASSWORD              password                        Cassandra password
CASSANDRA_KEY_SPACE             monasca                         Keyspace name where metrics are stored
CASSANDRA_CONNECTION_TIMEOUT    5                               Cassandra timeout in seconds
CASSANDRA_RETENTION_POLICY      45                              Data retention period in days
STAY_ALIVE_ON_FAILURE           false                           If true, container runs 2 hours even start fails
=============================== =============================== ================================================


Provide Configuration templates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* persister.conf.j2


Links
~~~~~
https://github.com/openstack/monasca-persister/tree/master/monasca_persister
