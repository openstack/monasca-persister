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


class DataPointsAsList(list):

    def append(self, key, value):
        super(DataPointsAsList, self).append(value)

    def get_count(self):
        return len(self)

    def clear(self):
        del self[:]


class DataPointsAsDict(dict):

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

    def append(self, key, value):
        super(DataPointsAsDict, self).setdefault(key, []).append(value)
        self.counter += 1

    def clear(self):
        super(DataPointsAsDict, self).clear()
        self.counter = 0

    def get_count(self):
        return self.counter
