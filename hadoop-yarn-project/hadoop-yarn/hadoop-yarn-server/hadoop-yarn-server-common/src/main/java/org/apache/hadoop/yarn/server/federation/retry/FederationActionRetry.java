package org.apache.hadoop.yarn.server.federation.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FederationActionRetry<T> {

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationActionRetry.class);

  protected abstract T run() throws Exception;

  public T runWithRetries(int retryCount, long retrySleepTime) throws Exception {
    int retry = 0;
    while (true) {
      try {
        return run();
      } catch (Exception e) {
        LOG.info("Exception while executing an Federation operation.", e);
        if (++retry > retryCount) {
          LOG.info("Maxed out Federation retries. Giving up!");
          throw e;
        }
        LOG.info("Retrying operation on Federation. Retry no. {}", retry);
        Thread.sleep(retrySleepTime);
      }
    }
  }
}
