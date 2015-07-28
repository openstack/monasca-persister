/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monasca.persister.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaConfig {

  @JsonProperty
  String topic;

  @JsonProperty
  String zookeeperConnect;

  @JsonProperty
  Integer socketTimeoutMs;

  @JsonProperty
  Integer socketReceiveBufferBytes;

  @JsonProperty
  Integer fetchMessageMaxBytes;

  @JsonProperty
  Integer queuedMaxMessageChunks;

  @JsonProperty
  Integer rebalanceMaxRetries;

  @JsonProperty
  Integer fetchMinBytes;

  @JsonProperty
  Integer fetchWaitMaxMs;

  @JsonProperty
  Integer rebalanceBackoffMs;

  @JsonProperty
  Integer refreshLeaderBackoffMs;

  @JsonProperty
  String autoOffsetReset;

  @JsonProperty
  Integer consumerTimeoutMs;

  @JsonProperty
  Integer zookeeperSessionTimeoutMs;

  @JsonProperty
  Integer zookeeperConnectionTimeoutMs;

  @JsonProperty
  Integer zookeeperSyncTimeMs;

  public String getTopic() {
    return topic;
  }

  public String getZookeeperConnect() {
    return zookeeperConnect;
  }

  public Integer getSocketTimeoutMs() {
    return socketTimeoutMs;
  }

  public Integer getSocketReceiveBufferBytes() {
    return socketReceiveBufferBytes;
  }

  public Integer getFetchMessageMaxBytes() {
    return fetchMessageMaxBytes;
  }

  public Integer getQueuedMaxMessageChunks() {
    return queuedMaxMessageChunks;
  }

  public Integer getRebalanceMaxRetries() {
    return rebalanceMaxRetries;
  }

  public Integer getFetchMinBytes() {
    return fetchMinBytes;
  }

  public Integer getFetchWaitMaxMs() {
    return fetchWaitMaxMs;
  }

  public Integer getRebalanceBackoffMs() {
    return rebalanceBackoffMs;
  }

  public Integer getRefreshLeaderBackoffMs() {
    return refreshLeaderBackoffMs;
  }

  public String getAutoOffsetReset() {
    return autoOffsetReset;
  }

  public Integer getConsumerTimeoutMs() {
    return consumerTimeoutMs;
  }

  public Integer getZookeeperSessionTimeoutMs() {
    return zookeeperSessionTimeoutMs;
  }

  public Integer getZookeeperConnectionTimeoutMs() {
    return zookeeperConnectionTimeoutMs;
  }

  public Integer getZookeeperSyncTimeMs() {
    return zookeeperSyncTimeMs;
  }
}
