==================================
Docker image for Monasca persister
==================================
The Monasca persister image is based on the monasca-base image.


Building monasca-base image
===========================
See https://github.com/openstack/monasca-common/tree/master/docker/README.rst


Building Docker image
=====================

Example:
  $ ./build_image.sh <repository_version> <upper_constrains_branch> <common_version>

Everything after ``./build_image.sh`` is optional and by default configured
to get versions from ``Dockerfile``. ``./build_image.sh`` also contain more
detailed build description.

Environment variables
~~~~~~~~~~~~~~~~~~~~~
=============================== ================= ================================================
Variable                        Default           Description
=============================== ================= ================================================
DEBUG                           false             If true, enable debug logging
VERBOSE                         true              If true, enable info logging
ZOOKEEPER_URI                   zookeeper:2181    The host and port for zookeeper
KAFKA_URI                       kafka:9092        The host and port for kafka
KAFKA_ALARM_HISTORY_BATCH_SIZE  1000              Kafka consumer takes messages in a batch
KAFKA_ALARM_HISTORY_GROUP_ID    1_events          Kafka Group from which persister get alarm history
KAFKA_ALARM_HISTORY_PROCESSORS  1                 Number of processes for alarm history topic
KAFKA_ALARM_HISTORY_WAIT_TIME   15                Seconds to wait if the batch size is not reached
KAFKA_EVENTS_ENABLE             false             Enable events persister
KAFKA_LEGACY_CLIENT_ENABLED     false             Enable legacy Kafka client
KAFKA_METRICS_BATCH_SIZE        1000              Kafka consumer takes messages in a batch
KAFKA_METRICS_GROUP_ID          1_metrics         Kafka Group from which persister get metrics
KAFKA_METRICS_PROCESSORS        1                 Number of processes for metrics topic
KAFKA_METRICS_WAIT_TIME         15                Seconds to wait if the batch size is not reached
DATABASE_BACKEND                influxdb          Select for backend database
INFLUX_HOST                     influxdb          The host for influxdb
INFLUX_PORT                     8086              The port for influxdb
INFLUX_USER                     mon_persister     The influx username
INFLUX_PASSWORD                 password          The influx password
INFLUX_DB                       mon               The influx database name
INFLUX_IGNORE_PARSE_POINT_ERROR false             Don't exit on InfluxDB parse point errors
CASSANDRA_HOSTS                 cassandra         Cassandra node addresses
CASSANDRA_PORT                  8086              Cassandra port number
CASSANDRA_USER                  mon_persister     Cassandra user name
CASSANDRA_PASSWORD              password          Cassandra password
CASSANDRA_KEY_SPACE             monasca           Keyspace name where metrics are stored
CASSANDRA_CONNECTION_TIMEOUT    5                 Cassandra timeout in seconds
CASSANDRA_MAX_CACHE_SIZE        20000000          Maximum number of cached metric definition entries in memory
CASSANDRA_RETENTION_POLICY      45                Data retention period in days
STAY_ALIVE_ON_FAILURE           false             If true, container runs 2 hours even start fails
=============================== ================= ================================================

Wait scripts environment variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
======================= ================================ =========================================
Variable                Default                          Description
======================= ================================ =========================================
KAFKA_URI               kafka:9092                       URI to Apache Kafka (distributed
                                                         streaming platform)
KAFKA_WAIT_FOR_TOPICS   alarm-state-transitions,metrics  The topics where metric-api streams
                                                         the metric messages and alarm-states
KAFKA_WAIT_RETRIES      24                               Number of kafka connect attempts
KAFKA_WAIT_DELAY        5                                Seconds to wait between attempts
======================= ================================ =========================================

Scripts
~~~~~~~
start.sh
  In this starting script provide all steps that lead to the proper service
  start. Including usage of wait scripts and templating of configuration
  files. You also could provide the ability to allow running container after
  service died for easier debugging.

build_image.sh
  Please read detailed build description inside the script.

Provide Configuration templates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* monasca-persister.conf.j2
* persister-logging.conf.j2


Links
~~~~~
https://github.com/openstack/monasca-persister/tree/master/monasca_persister
