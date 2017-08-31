/*
 * Copyright (c) 2017 SUSE LLC
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

package monasca.persister.repository.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class CassandraMetricBatch {
  private static Logger logger = LoggerFactory.getLogger(CassandraMetricBatch.class);

  ProtocolOptions protocol;
  CodecRegistry codec;
  Metadata metadata;
  TokenAwarePolicy policy;
  int batchLimit;

  Map<Token, Deque<BatchStatement>> metricQueries;
  Map<Token, Deque<BatchStatement>> dimensionQueries;
  Map<Token, Deque<BatchStatement>> dimensionMetricQueries;
  Map<Token, Deque<BatchStatement>> metricDimensionQueries;
  Map<Set<Host>, Deque<BatchStatement>> measurementQueries;

  public CassandraMetricBatch(Metadata metadata, ProtocolOptions protocol, CodecRegistry codec,
      TokenAwarePolicy lbPolicy, int batchLimit) {
    this.protocol = protocol;
    this.codec = codec;
    this.metadata = metadata;
    this.policy = lbPolicy;
    metricQueries = new HashMap<>();
    this.batchLimit = batchLimit;

    metricQueries = new HashMap<>();
    dimensionQueries = new HashMap<>();
    dimensionMetricQueries = new HashMap<>();
    metricDimensionQueries = new HashMap<>();
    measurementQueries = new HashMap<>();
  }

  public void addMetricQuery(BoundStatement s) {
    batchQueryByToken(s, metricQueries);
  }

  public void addDimensionQuery(BoundStatement s) {
    batchQueryByToken(s, dimensionQueries);
  }

  public void addDimensionMetricQuery(BoundStatement s) {
    batchQueryByToken(s, dimensionMetricQueries);
  }

  public void addMetricDimensionQuery(BoundStatement s) {
    batchQueryByToken(s, metricDimensionQueries);
  }

  public void addMeasurementQuery(BoundStatement s) {
    batchQueryByReplica(s, measurementQueries);
  }

  private void batchQueryByToken(BoundStatement s, Map<Token, Deque<BatchStatement>> batchedQueries) {
    ByteBuffer b = s.getRoutingKey(protocol.getProtocolVersion(), codec);
    Token token = metadata.newToken(b);
    Deque<BatchStatement> queue = batchedQueries.get(token);
    if (queue == null) {
      queue = new ArrayDeque<BatchStatement>();
      BatchStatement bs = new BatchStatement(Type.UNLOGGED);
      bs.add(s);
      queue.offer(bs);
      batchedQueries.put(token, queue);
    } else {
      BatchStatement bs = queue.getLast();
      if (bs.size() < batchLimit) {
        bs.add(s);
      } else {
        bs = new BatchStatement(Type.UNLOGGED);
        bs.add(s);
        queue.offerLast(bs);
      }
    }
  }

  private void batchQueryByReplica(BoundStatement s,
      Map<Set<Host>, Deque<BatchStatement>> batchedQueries) {
    Iterator<Host> it = policy.newQueryPlan(s.getKeyspace(), s);
    Set<Host> hosts = new HashSet<>();

    while (it.hasNext()) {
      hosts.add(it.next());
    }

    Deque<BatchStatement> queue = batchedQueries.get(hosts);
    if (queue == null) {
      queue = new ArrayDeque<BatchStatement>();
      BatchStatement bs = new BatchStatement(Type.UNLOGGED);
      bs.add(s);
      queue.offer(bs);
      batchedQueries.put(hosts, queue);
    } else {
      BatchStatement bs = queue.getLast();
      if (bs.size() < 30) {
        bs.add(s);
      } else {
        bs = new BatchStatement(Type.UNLOGGED);
        bs.add(s);
        queue.offerLast(bs);
      }
    }
  }

  public void clear() {
    metricQueries.clear();
    dimensionQueries.clear();
    dimensionMetricQueries.clear();
    metricDimensionQueries.clear();
    measurementQueries.clear();
  }

  public List<Deque<BatchStatement>> getAllBatches() {
    logTokenBatchMap("metric batches", metricQueries);
    logTokenBatchMap("dimension batches", dimensionQueries);
    logTokenBatchMap("dimension metric batches", dimensionMetricQueries);
    logTokenBatchMap("metric dimension batches", metricDimensionQueries);
    logReplicaBatchMap("measurement batches", measurementQueries);

    ArrayList<Deque<BatchStatement>> list = new ArrayList<>();
    list.addAll(metricQueries.values());
    list.addAll(dimensionQueries.values());
    list.addAll(dimensionMetricQueries.values());
    list.addAll(metricDimensionQueries.values());
    list.addAll(measurementQueries.values());
    return list;
  }

  private void logTokenBatchMap(String name, Map<Token, Deque<BatchStatement>> map) {
    if (logger.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder(name);
      sb.append(": Size: ").append(map.size());
      sb.append(";  Tokens: |");
      for (Entry<Token, Deque<BatchStatement>> entry : map.entrySet()) {
        sb.append(entry.getKey().toString()).append(":");
        for (BatchStatement bs : entry.getValue()) {
          sb.append(bs.size()).append(",");
        }
        sb.append("|.");
      }

      logger.debug(sb.toString());
    }
  }

  private void logReplicaBatchMap(String name, Map<Set<Host>, Deque<BatchStatement>> map) {
    if (logger.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder(name);
      sb.append(": Size: ").append(map.size());
      sb.append(". Replicas: |");
      for (Entry<Set<Host>, Deque<BatchStatement>> entry : map.entrySet()) {
        for (Host host : entry.getKey()) {
          sb.append(host.getAddress().toString()).append(",");
        }
        sb.append(":");
        for (BatchStatement bs : entry.getValue()) {
          sb.append(bs.size()).append(",");
        }

        sb.append("|");

      }
      logger.debug(sb.toString());
    }
  }
}
