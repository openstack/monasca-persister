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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown=true)
public class KafkaConfig {

  @JsonProperty
  String topic;

  @JsonProperty
  String zookeeperConnect;
  String _zookeeperConnect = "127.0.0.1";

  @JsonProperty
  Integer socketTimeoutMs;
  Integer _socketTimeoutMs = 30000;

  @JsonProperty
  Integer socketReceiveBufferBytes;
  Integer _socketReceiveBufferBytes = 65536;

  @JsonProperty
  Integer fetchMessageMaxBytes;
  Integer _fetchMessageMaxBytes = 1048576;

  @JsonProperty
  Integer queuedMaxMessageChunks;
  Integer _queuedMaxMessageChunks = 10;

  @JsonProperty
  Integer rebalanceMaxRetries;
  Integer _rebalanceMaxRetries = 4;

  @JsonProperty
  Integer fetchMinBytes;
  Integer _fetchMinBytes = 1;

  @JsonProperty
  Integer fetchWaitMaxMs;
  Integer _fetchWaitMaxMs = 100;

  @JsonProperty
  Integer rebalanceBackoffMs;
  Integer _rebalanceBackoffMs = 2000;

  @JsonProperty
  Integer refreshLeaderBackoffMs;
  Integer _refreshLeaderBackoffMs = 200;

  @JsonProperty
  String autoOffsetReset;
  String _autoOffsetReset = "largest";

  @JsonProperty
  Integer consumerTimeoutMs;
  Integer _consumerTimeoutMs = 1000;

  @JsonProperty
  Integer zookeeperSessionTimeoutMs;
  Integer _zookeeperSessionTimeoutMs = 60000;

  @JsonProperty
  Integer zookeeperConnectionTimeoutMs;
  Integer _zookeeperConnectionTimeoutMs = 60000;

  @JsonProperty
  Integer zookeeperSyncTimeMs;
  Integer _zookeeperSyncTimeMs = 2000;

  public String getTopic() {
    return topic;
  }

  public String getZookeeperConnect() {
    if ( zookeeperConnect == null ) {
      return _zookeeperConnect;
    }
    return zookeeperConnect;
  }

  public Integer getSocketTimeoutMs() {
    if ( socketTimeoutMs == null ) {
      return _socketTimeoutMs;
    }
    return socketTimeoutMs;
  }

  public Integer getSocketReceiveBufferBytes() {
    if ( socketReceiveBufferBytes == null ) {
      return _socketReceiveBufferBytes;
    }
    return socketReceiveBufferBytes;
  }

  public Integer getFetchMessageMaxBytes() {
    if ( fetchMessageMaxBytes == null ) {
      return _fetchMessageMaxBytes;
    }
    return fetchMessageMaxBytes;
  }

  public Integer getQueuedMaxMessageChunks() {
    if ( queuedMaxMessageChunks == null ) {
      return _queuedMaxMessageChunks;
    }
    return queuedMaxMessageChunks;
  }

  public Integer getRebalanceMaxRetries() {
    if ( rebalanceMaxRetries == null ) {
      return _rebalanceMaxRetries;
    }
    return rebalanceMaxRetries;
  }

  public Integer getFetchMinBytes() {
    if ( fetchMinBytes == null ) {
      return _fetchMinBytes;
    }
    return fetchMinBytes;
  }

  public Integer getFetchWaitMaxMs() {
    if ( fetchWaitMaxMs == null ) {
      return _fetchWaitMaxMs;
    }
    return fetchWaitMaxMs;
  }

  public Integer getRebalanceBackoffMs() {
    if ( rebalanceBackoffMs == null ) {
      return _rebalanceBackoffMs;
    }
    return rebalanceBackoffMs;
  }

  public Integer getRefreshLeaderBackoffMs() {
    if ( refreshLeaderBackoffMs == null ) {
      return _refreshLeaderBackoffMs;
    }
    return refreshLeaderBackoffMs;
  }

  public String getAutoOffsetReset() {
    if ( autoOffsetReset == null ) {
      return _autoOffsetReset;
    }
    return autoOffsetReset;
  }

  public Integer getConsumerTimeoutMs() {
    if ( consumerTimeoutMs == null ) {
      return _consumerTimeoutMs;
    }
    return consumerTimeoutMs;
  }

  public Integer getZookeeperSessionTimeoutMs() {
    if ( zookeeperSessionTimeoutMs == null ) {
      return _zookeeperSessionTimeoutMs;
    }
    return zookeeperSessionTimeoutMs;
  }

  public Integer getZookeeperConnectionTimeoutMs() {
    if ( zookeeperConnectionTimeoutMs == null ) {
      return _zookeeperConnectionTimeoutMs;
    }
    return zookeeperConnectionTimeoutMs;
  }

  public Integer getZookeeperSyncTimeMs() {
    if ( zookeeperSyncTimeMs == null ) {
      return _zookeeperSyncTimeMs;
    }
    return zookeeperSyncTimeMs;
  }
}
