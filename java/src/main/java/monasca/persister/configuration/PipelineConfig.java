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
public class PipelineConfig {

  @JsonProperty
  String topic;
  String _topic; // No default: default provided by constructor

  @JsonProperty
  String groupId;
  String _groupId; // No default: default provided by constructor

  @JsonProperty
  String consumerId;
  String _consumerId = "monasca-persister";

  @JsonProperty
  String clientId;
  String _clientId = "monasca-persister";

  @JsonProperty
  Integer batchSize;
  Integer _batchSize; // No default: default provided by constructor

  @JsonProperty
  Integer numThreads;
  Integer _numThreads = 1;

  @JsonProperty
  Integer maxBatchTime;
  Integer _maxBatchTime = 10;

  @JsonProperty
  Integer commitBatchTime;
  Integer _commitBatchTime = 0;

  /** Used to set default values for properties that have different sensible
   * defaults for metric and alarm configurations, respectively.
   */
  public void setDefaults(String defaultTopic, String defaultGroupId,
                          Integer defaultBatchSize) {
    _batchSize = defaultBatchSize;
    _groupId = defaultGroupId;
    _topic = defaultTopic;
  }

  public Integer getCommitBatchTime() {
    if ( commitBatchTime == null ) {
      return _commitBatchTime;
    }
    return commitBatchTime;
  }

  public void setCommitBatchTime(Integer commitBatchTime) {
    this.commitBatchTime = commitBatchTime;
  }

  public String getTopic() {
    if ( topic == null ) {
      return _topic;
    }
    return topic;
  }

  public String getGroupId() {
    if ( groupId == null ) {
      return _groupId;
    }
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public String getConsumerId() {
    if ( consumerId == null ) {
      return _consumerId;
    }
    return consumerId;
  }

  public void setConsumerId(String consumerId) {
    this.consumerId = consumerId;
  }

  public String getClientId() {
    if ( clientId == null ) {
      return _clientId;
    }
    return clientId;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public void setBatchSize(Integer batchSize) {
    this.batchSize = batchSize;
  }

  public void setNumThreads(Integer numThreads) {
    this.numThreads = numThreads;
  }

  public void setMaxBatchTime(Integer maxBatchTime) {
    this.maxBatchTime = maxBatchTime;
  }

  public Integer getBatchSize() {
    if ( batchSize == null ) {
      return _batchSize;
    }
    return batchSize;
  }

  public Integer getNumThreads() {
    if ( numThreads == null ) {
      return _numThreads;
    }
    return numThreads;
  }

  public Integer getMaxBatchTime() {
    if ( maxBatchTime == null ) {
      return _maxBatchTime;
    }
    return maxBatchTime;
  }
}
