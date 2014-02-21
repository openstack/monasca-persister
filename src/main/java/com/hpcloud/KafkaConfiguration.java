package com.hpcloud;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaConfiguration {

    @JsonProperty
    String topic;

    @JsonProperty
    Integer numThreads;

    @JsonProperty
    String groupId;

    @JsonProperty
    String zookeeperConnect;

    @JsonProperty
    String consumerId;

    @JsonProperty
    Integer socketTimeoutMs;

    @JsonProperty
    Integer socketReceiveBufferBytes;

    @JsonProperty
    Integer fetchMessageMaxBytes;

    @JsonProperty
    Boolean autoCommitEnable;

    @JsonProperty
    Integer autoCommitIntervalMs;

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
    String clientId;

    @JsonProperty
    Integer zookeeperSessionTimeoutMs;

    @JsonProperty
    Integer zookeeperConnectionTimeoutMs;

    @JsonProperty
    Integer zookeeperSyncTimeMs;
}
