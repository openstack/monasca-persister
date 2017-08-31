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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.BootstrappingException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.QueryConsistencyException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.dropwizard.setup.Environment;
import monasca.persister.repository.RepoException;

public abstract class CassandraRepo {
  private static Logger logger = LoggerFactory.getLogger(CassandraRepo.class);

  final Environment environment;

  final Timer commitTimer;

  CassandraCluster cluster;
  Session session;

  int maxWriteRetries;

  int batchSize;

  long lastFlushTimeStamp;

  Deque<Statement> queue;

  Counter metricCompleted;

  Counter metricFailed;

  public CassandraRepo(CassandraCluster cluster, Environment env, int maxWriteRetries, int batchSize) {
    this.cluster = cluster;
    this.maxWriteRetries = maxWriteRetries;
    this.batchSize = batchSize;

    this.environment = env;

    this.commitTimer = this.environment.metrics().timer(getClass().getName() + "." + "commit-timer");

    lastFlushTimeStamp = System.currentTimeMillis();

    queue = new ArrayDeque<>(batchSize);

    this.metricCompleted = environment.metrics()
        .counter(getClass().getName() + "." + "metrics-persisted-counter");

    this.metricFailed = environment.metrics()
        .counter(getClass().getName() + "." + "metrics-failed-counter");
  }

  protected void executeQuery(String id, Statement query, long startTime) throws DriverException {
    _executeQuery(id, query, startTime, 0);
  }

  private void _executeQuery(final String id, final Statement query, final long startTime,
      final int retryCount) {
    try {
      session.execute(query);

      commitTimer.update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

      // ResultSetFuture future = session.executeAsync(query);

      // Futures.addCallback(future, new FutureCallback<ResultSet>() {
      // @Override
      // public void onSuccess(ResultSet result) {
      // metricCompleted.inc();
      // commitTimer.update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
      // }
      //
      // @Override
      // public void onFailure(Throwable t) {
      // if (t instanceof NoHostAvailableException | t instanceof
      // BootstrappingException
      // | t instanceof OverloadedException | t instanceof QueryConsistencyException
      // | t instanceof UnavailableException) {
      // retryQuery(id, query, startTime, retryCount, (DriverException) t);
      // } else {
      // metricFailed.inc();
      // commitTimer.update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
      // logger.error("Failed to execute query.", t);
      // }
      // }
      // }, MoreExecutors.sameThreadExecutor());

    } catch (NoHostAvailableException | BootstrappingException | OverloadedException
        | QueryConsistencyException | UnavailableException | OperationTimedOutException e) {
      retryQuery(id, query, startTime, retryCount, e);
    } catch (DriverException e) {
      metricFailed.inc(((BatchStatement) query).size());
      commitTimer.update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
      throw e;
    }
  }

  private void retryQuery(String id, Statement query, final long startTime, int retryCount,
      DriverException e) throws DriverException {
    if (retryCount >= maxWriteRetries) {
      logger.error("[{}]: Query aborted after {} retry: ", id, retryCount, e.getMessage());
      metricFailed.inc(((BatchStatement) query).size());
      commitTimer.update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
      throw e;
    } else {
      logger.warn("[{}]: Query failed, retrying {} of {}: {} ", id, retryCount, maxWriteRetries,
          e.getMessage());

      try {
        Thread.sleep(1000 * (1 << retryCount));
      } catch (InterruptedException ie) {
        logger.debug("[{}]: Interrupted: {}", id, ie);
      }
      _executeQuery(id, query, startTime, retryCount++);
    }
  }

  public int handleFlush_batch(String id) {
    Statement query;
    int flushedCount = 0;

    BatchStatement batch = new BatchStatement(Type.UNLOGGED);
    while ((query = queue.poll()) != null) {
      flushedCount++;
      batch.add(query);
    }

    executeQuery(id, batch, System.nanoTime());

    metricCompleted.inc(flushedCount);

    return flushedCount;
  }

  public int handleFlush(String id) throws RepoException {
    long startTime = System.nanoTime();

    int flushedCount = 0;
    List<ResultSetFuture> results = new ArrayList<>(queue.size());
    Statement query;
    while ((query = queue.poll()) != null) {
      flushedCount++;
      results.add(session.executeAsync(query));
    }

    List<ListenableFuture<ResultSet>> futures = Futures.inCompletionOrder(results);

    boolean cancel = false;
    Exception ex = null;
    for (ListenableFuture<ResultSet> future : futures) {
      if (cancel) {
        future.cancel(false);
        continue;
      }
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        cancel = true;
        ex = e;
      }
    }

    commitTimer.update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

    if (ex != null) {
      throw new RepoException(ex);
    }
    return flushedCount;
  }
}
