# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
# (C) Copyright 2017 SUSE LLC
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
from abc import ABCMeta
import os

from oslo_config import cfg
from oslo_log import log
import six

from monasca_persister.repositories import singleton

LOG = log.getLogger(__name__)

class DataPoints(dict):

    def __init__(self):
        self.counter = 0

    def __setitem__(self, key, value):
        raise NotImplementedError('Use append(key, value) instead.')

    def __delitem__(self, key):
        raise NotImplementedError('Use clear() instead.')

    def pop(self):
        raise NotImplementedError('Use clear() instead.')

    def popitem(self):
        raise NotImplementedError('Use clear() instead.')

    def update(self):
        raise NotImplementedError('Use clear() instead.')

    def chained(self):
        return [vi for vo in super(DataPoints, self).values() for vi in vo]

    def append(self, key, value):
        super(DataPoints, self).setdefault(key, []).append(value)
        self.counter += 1

    def clear(self):
        super(DataPoints, self).clear()
        self.counter = 0


@six.add_metaclass(singleton.Singleton)
class Persister(six.with_metaclass(ABCMeta, object)):

    def __init__(self, kafka_conf, repository):
        self._data_points = DataPoints()

        self._kafka_topic = kafka_conf.topic
        self._batch_size = kafka_conf.batch_size
        self.repository = repository()

    def _flush(self):
        if not self._data_points:
            return

        try:
            self.repository.write_batch(self._data_points)

            LOG.info("Processed {} messages from topic '{}'".format(
                self._data_points.counter, self._kafka_topic))

            self._data_points.clear()
            self._consumer.commit()
        except Exception as ex:
            if "partial write: points beyond retention policy dropped" in str(ex):
                LOG.warning("Some points older than retention policy were dropped")
                self._data_points.clear()
                self._consumer.commit()

            elif cfg.CONF.repositories.ignore_parse_point_error \
                    and "unable to parse" in str(ex):
                LOG.warning("Some points were unable to be parsed and were dropped")
                self._data_points.clear()
                self._consumer.commit()

            else:
                LOG.exception("Error writing to database: {}"
                              .format(self._data_points))
                raise ex

    def run(self):
        try:
            for message in self._consumer:
                try:
                    data_point, tenant_id = self.repository.process_message(message)
                    self._data_points.append(tenant_id, data_point)
                except Exception:
                    LOG.exception('Error processing message. Message is '
                                  'being dropped. {}'.format(message))

                if self._data_points.counter >= self._batch_size:
                    self._flush()
        except Exception:
            LOG.exception(
                'Persister encountered fatal exception processing '
                'messages. '
                'Shutting down all threads and exiting')
            os._exit(1)
