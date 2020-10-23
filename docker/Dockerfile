ARG DOCKER_IMAGE=monasca/persister
ARG APP_REPO=https://review.opendev.org/openstack/monasca-persister

# Branch, tag or git hash to build from.
ARG REPO_VERSION=master
ARG CONSTRAINTS_BRANCH=master

# Extra Python3 dependencies.
ARG EXTRA_DEPS="influxdb"

# Always start from `monasca-base` image and use specific tag of it.
ARG BASE_TAG=master
FROM monasca/base:$BASE_TAG

# Environment variables used for our service or wait scripts.
ENV \
    DEBUG=false \
    VERBOSE=true \
    LOG_LEVEL=WARNING \
    LOG_LEVEL_KAFKA=WARNING \
    LOG_LEVEL_INFLUXDB=WARNING \
    LOG_LEVEL_CASSANDRA=WARNING \
    ZOOKEEPER_URI=zookeeper:2181 \
    KAFKA_URI=kafka:9092 \
    KAFKA_ALARM_HISTORY_BATCH_SIZE=1000 \
    KAFKA_ALARM_HISTORY_GROUP_ID=1_events \
    KAFKA_ALARM_HISTORY_PROCESSORS=1 \
    KAFKA_ALARM_HISTORY_WAIT_TIME=15 \
    KAFKA_EVENTS_ENABLE="false" \
    KAFKA_LEGACY_CLIENT_ENABLED=false \
    KAFKA_METRICS_BATCH_SIZE=1000 \
    KAFKA_METRICS_GROUP_ID=1_metrics \
    KAFKA_METRICS_PROCESSORS=1 \
    KAFKA_METRICS_WAIT_TIME=15 \
    KAFKA_WAIT_FOR_TOPICS=alarm-state-transitions,metrics \
    DATABASE_BACKEND=influxdb \
    INFLUX_HOST=influxdb \
    INFLUX_PORT=8086 \
    INFLUX_USER=mon_persister \
    INFLUX_PASSWORD=password \
    INFLUX_DB=mon \
    INFLUX_IGNORE_PARSE_POINT_ERROR="false" \
    CASSANDRA_HOSTS=cassandra \
    CASSANDRA_PORT=8086 \
    CASSANDRA_USER=mon_persister \
    CASSANDRA_PASSWORD=password \
    CASSANDRA_KEY_SPACE=monasca \
    CASSANDRA_CONNECTION_TIMEOUT=5 \
    CASSANDRA_MAX_CACHE_SIZE=20000000 \
    CASSANDRA_RETENTION_POLICY=45 \
    STAY_ALIVE_ON_FAILURE="false"

# Copy all necessary files to proper locations.
COPY monasca-persister.conf.j2 persister-logging.conf.j2 /etc/monasca/

# Run here all additional steps your service need post installation.
# Stay with only one `RUN` and use `&& \` for next steps to don't create
# unnecessary image layers. Clean at the end to conserve space.
#RUN \
#    echo "Some steps to do after main installation." && \
#    echo "Hello when building."

# Expose port for specific service.
#EXPOSE 1234

# Implement start script in `start.sh` file.
CMD ["/start.sh"]
