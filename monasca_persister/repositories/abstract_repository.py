# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
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
import abc
import six


@six.add_metaclass(abc.ABCMeta)
class AbstractRepository(object):

    def __init__(self):
        super(AbstractRepository, self).__init__()

    @abc.abstractmethod
    def process_message(self, message):
        pass

    @abc.abstractmethod
    def write_batch(self, data_points):
        pass
