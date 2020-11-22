#  Licensed under the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License. You may obtain
#  a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#  License for the specific language governing permissions and limitations
#  under the License.
from monasca_common.kafka import client_factory

from monasca_persister.repositories import persister
from monasca_persister.repositories import singleton


class ConfluentKafkaPersister(persister.Persister, metaclass=singleton.Singleton):

    def __init__(self, kafka_conf, repository, client_id=""):
        super(ConfluentKafkaPersister, self).__init__(kafka_conf, repository)
        self._consumer = client_factory.get_kafka_consumer(
            kafka_url=kafka_conf.uri,
            kafka_consumer_group=kafka_conf.group_id,
            kafka_topic=kafka_conf.topic,
            client_id=client_id,
            repartition_callback=ConfluentKafkaPersister.flush,
            commit_callback=self._flush,
            max_commit_interval=kafka_conf.max_wait_time_seconds
        )

    @staticmethod
    def flush(kafka_consumer, partitions):
        p = ConfluentKafkaPersister()
        p._flush()
