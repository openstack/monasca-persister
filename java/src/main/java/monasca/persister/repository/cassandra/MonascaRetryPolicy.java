package monasca.persister.repository.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

public class MonascaRetryPolicy implements RetryPolicy {

  private final int readAttempts;
  private final int writeAttempts;
  private final int unavailableAttempts;

  public MonascaRetryPolicy(int readAttempts, int writeAttempts, int unavailableAttempts) {
    super();
    this.readAttempts = readAttempts;
    this.writeAttempts = writeAttempts;
    this.unavailableAttempts = unavailableAttempts;
  }

  @Override
  public RetryDecision onReadTimeout(Statement stmnt, ConsistencyLevel cl, int requiredResponses,
      int receivedResponses, boolean dataReceived, int rTime) {
    if (dataReceived) {
      return RetryDecision.ignore();
    } else if (rTime < readAttempts) {
      return receivedResponses >= requiredResponses ? RetryDecision.retry(cl)
          : RetryDecision.rethrow();
    } else {
      return RetryDecision.rethrow();
    }

  }

  @Override
  public RetryDecision onWriteTimeout(Statement stmnt, ConsistencyLevel cl, WriteType wt,
      int requiredResponses, int receivedResponses, int wTime) {
    if (wTime >= writeAttempts)
      return RetryDecision.rethrow();

    return RetryDecision.retry(cl);
  }

  @Override
  public RetryDecision onUnavailable(Statement stmnt, ConsistencyLevel cl, int requiredResponses,
      int receivedResponses, int uTime) {
    if (uTime == 0) {
      return RetryDecision.tryNextHost(cl);
    } else if (uTime <= unavailableAttempts) {
      return RetryDecision.retry(cl);
    } else {
      return RetryDecision.rethrow();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e,
      int nbRetry) {
    return RetryDecision.tryNextHost(cl);
  }

  @Override
  public void init(Cluster cluster) {
    // nothing to do
  }

  @Override
  public void close() {
    // nothing to do
  }

}
