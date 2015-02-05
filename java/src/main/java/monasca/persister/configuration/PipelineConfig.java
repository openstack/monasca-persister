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

public class PipelineConfig {

  @JsonProperty
  String topic;

  @JsonProperty
  String groupId;

  @JsonProperty
  String consumerId;

  @JsonProperty
  String clientId;

  @JsonProperty
  Integer batchSize;

  @JsonProperty
  Integer numThreads;

  @JsonProperty
  Integer maxBatchTime;

  public String getTopic() {
    return topic;
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(String consumerId) {
    this.consumerId = consumerId;
  }

  public String getClientId() {
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
    return batchSize;
  }

  public Integer getNumThreads() {
    return numThreads;
  }

  public Integer getMaxBatchTime() {
    return maxBatchTime;
  }
}
