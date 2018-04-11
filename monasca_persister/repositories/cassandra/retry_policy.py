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

from cassandra.policies import RetryPolicy


class MonascaRetryPolicy(RetryPolicy):

    def __init__(self, read_attempts, write_attempts, unavailable_attempts):

        super(MonascaRetryPolicy, self).__init__()

        self.read_attempts = read_attempts
        self.write_attempts = write_attempts
        self.unavailable_attempts = unavailable_attempts

    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):

        if retry_num >= self.read_attempts:
            return self.RETHROW, None
        elif received_responses >= required_responses and not data_retrieved:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None

    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):

        if retry_num >= self.write_attempts:
            return self.RETHROW, None
        else:
            return self.RETRY, consistency

    def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):

        return (
            self.RETRY_NEXT_HOST,
            consistency) if retry_num < self.unavailable_attempts else (
            self.RETHROW,
            None)
