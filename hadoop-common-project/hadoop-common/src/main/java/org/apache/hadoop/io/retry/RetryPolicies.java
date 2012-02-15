/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.retry;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;

import com.google.common.annotations.VisibleForTesting;

/**
 * <p>
 * A collection of useful implementations of {@link RetryPolicy}.
 * </p>
 */
public class RetryPolicies {
  
  public static final Log LOG = LogFactory.getLog(RetryPolicies.class);
  
  private static final Random RAND = new Random();
  
  /**
   * <p>
   * Try once, and fail by re-throwing the exception.
   * This corresponds to having no retry mechanism in place.
   * </p>
   */
  public static final RetryPolicy TRY_ONCE_THEN_FAIL = new TryOnceThenFail();
  
  /**
   * <p>
   * Keep trying forever.
   * </p>
   */
  public static final RetryPolicy RETRY_FOREVER = new RetryForever();
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a fixed time between attempts,
   * and then fail by re-throwing the exception.
   * </p>
   */
  public static final RetryPolicy retryUpToMaximumCountWithFixedSleep(int maxRetries, long sleepTime, TimeUnit timeUnit) {
    return new RetryUpToMaximumCountWithFixedSleep(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying for a maximum time, waiting a fixed time between attempts,
   * and then fail by re-throwing the exception.
   * </p>
   */
  public static final RetryPolicy retryUpToMaximumTimeWithFixedSleep(long maxTime, long sleepTime, TimeUnit timeUnit) {
    return new RetryUpToMaximumTimeWithFixedSleep(maxTime, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a growing amount of time between attempts,
   * and then fail by re-throwing the exception.
   * The time between attempts is <code>sleepTime</code> mutliplied by the number of tries so far.
   * </p>
   */
  public static final RetryPolicy retryUpToMaximumCountWithProportionalSleep(int maxRetries, long sleepTime, TimeUnit timeUnit) {
    return new RetryUpToMaximumCountWithProportionalSleep(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a growing amount of time between attempts,
   * and then fail by re-throwing the exception.
   * The time between attempts is <code>sleepTime</code> mutliplied by a random
   * number in the range of [0, 2 to the number of retries)
   * </p>
   */
  public static final RetryPolicy exponentialBackoffRetry(
      int maxRetries, long sleepTime, TimeUnit timeUnit) {
    return new ExponentialBackoffRetry(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Set a default policy with some explicit handlers for specific exceptions.
   * </p>
   */
  public static final RetryPolicy retryByException(RetryPolicy defaultPolicy,
                                                   Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap) {
    return new ExceptionDependentRetry(defaultPolicy, exceptionToPolicyMap);
  }
  
  /**
   * <p>
   * A retry policy for RemoteException
   * Set a default policy with some explicit handlers for specific exceptions.
   * </p>
   */
  public static final RetryPolicy retryByRemoteException(
      RetryPolicy defaultPolicy,
      Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap) {
    return new RemoteExceptionDependentRetry(defaultPolicy, exceptionToPolicyMap);
  }
  
  public static final RetryPolicy failoverOnNetworkException(int maxFailovers) {
    return failoverOnNetworkException(TRY_ONCE_THEN_FAIL, maxFailovers);
  }
  
  public static final RetryPolicy failoverOnNetworkException(
      RetryPolicy fallbackPolicy, int maxFailovers) {
    return failoverOnNetworkException(fallbackPolicy, maxFailovers, 0, 0);
  }
  
  public static final RetryPolicy failoverOnNetworkException(
      RetryPolicy fallbackPolicy, int maxFailovers, long delayMillis,
      long maxDelayBase) {
    return new FailoverOnNetworkExceptionRetry(fallbackPolicy, maxFailovers,
        delayMillis, maxDelayBase);
  }
  
  static class TryOnceThenFail implements RetryPolicy {
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isMethodIdempotent) throws Exception {
      return RetryAction.FAIL;
    }
  }
  
  static class RetryForever implements RetryPolicy {
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isMethodIdempotent) throws Exception {
      return RetryAction.RETRY;
    }
  }
  
  static abstract class RetryLimited implements RetryPolicy {
    int maxRetries;
    long sleepTime;
    TimeUnit timeUnit;
    
    public RetryLimited(int maxRetries, long sleepTime, TimeUnit timeUnit) {
      this.maxRetries = maxRetries;
      this.sleepTime = sleepTime;
      this.timeUnit = timeUnit;
    }

    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isMethodIdempotent) throws Exception {
      if (retries >= maxRetries) {
        return RetryAction.FAIL;
      }
      return new RetryAction(RetryAction.RetryDecision.RETRY,
          timeUnit.toMillis(calculateSleepTime(retries)));
    }
    
    protected abstract long calculateSleepTime(int retries);
  }
  
  static class RetryUpToMaximumCountWithFixedSleep extends RetryLimited {
    public RetryUpToMaximumCountWithFixedSleep(int maxRetries, long sleepTime, TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);
    }
    
    @Override
    protected long calculateSleepTime(int retries) {
      return sleepTime;
    }
  }
  
  static class RetryUpToMaximumTimeWithFixedSleep extends RetryUpToMaximumCountWithFixedSleep {
    public RetryUpToMaximumTimeWithFixedSleep(long maxTime, long sleepTime, TimeUnit timeUnit) {
      super((int) (maxTime / sleepTime), sleepTime, timeUnit);
    }
  }
  
  static class RetryUpToMaximumCountWithProportionalSleep extends RetryLimited {
    public RetryUpToMaximumCountWithProportionalSleep(int maxRetries, long sleepTime, TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);
    }
    
    @Override
    protected long calculateSleepTime(int retries) {
      return sleepTime * (retries + 1);
    }
  }
  
  static class ExceptionDependentRetry implements RetryPolicy {

    RetryPolicy defaultPolicy;
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap;
    
    public ExceptionDependentRetry(RetryPolicy defaultPolicy,
                                   Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap) {
      this.defaultPolicy = defaultPolicy;
      this.exceptionToPolicyMap = exceptionToPolicyMap;
    }

    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isMethodIdempotent) throws Exception {
      RetryPolicy policy = exceptionToPolicyMap.get(e.getClass());
      if (policy == null) {
        policy = defaultPolicy;
      }
      return policy.shouldRetry(e, retries, failovers, isMethodIdempotent);
    }
    
  }
  
  static class RemoteExceptionDependentRetry implements RetryPolicy {

    RetryPolicy defaultPolicy;
    Map<String, RetryPolicy> exceptionNameToPolicyMap;
    
    public RemoteExceptionDependentRetry(RetryPolicy defaultPolicy,
                                   Map<Class<? extends Exception>,
                                   RetryPolicy> exceptionToPolicyMap) {
      this.defaultPolicy = defaultPolicy;
      this.exceptionNameToPolicyMap = new HashMap<String, RetryPolicy>();
      for (Entry<Class<? extends Exception>, RetryPolicy> e :
          exceptionToPolicyMap.entrySet()) {
        exceptionNameToPolicyMap.put(e.getKey().getName(), e.getValue());
      }
    }

    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isMethodIdempotent) throws Exception {
      RetryPolicy policy = null;
      if (e instanceof RemoteException) {
        policy = exceptionNameToPolicyMap.get(
            ((RemoteException) e).getClassName());
      }
      if (policy == null) {
        policy = defaultPolicy;
      }
      return policy.shouldRetry(e, retries, failovers, isMethodIdempotent);
    }
  }
  
  static class ExponentialBackoffRetry extends RetryLimited {
    
    public ExponentialBackoffRetry(
        int maxRetries, long sleepTime, TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);
    }
    
    @Override
    protected long calculateSleepTime(int retries) {
      return calculateExponentialTime(sleepTime, retries + 1);
    }
  }
  
  /**
   * Fail over and retry in the case of:
   *   Remote StandbyException (server is up, but is not the active server)
   *   Immediate socket exceptions (e.g. no route to host, econnrefused)
   *   Socket exceptions after initial connection when operation is idempotent
   * 
   * The first failover is immediate, while all subsequent failovers wait an
   * exponentially-increasing random amount of time.
   * 
   * Fail immediately in the case of:
   *   Socket exceptions after initial connection when operation is not idempotent
   * 
   * Fall back on underlying retry policy otherwise.
   */
  static class FailoverOnNetworkExceptionRetry implements RetryPolicy {
    
    private RetryPolicy fallbackPolicy;
    private int maxFailovers;
    private long delayMillis;
    private long maxDelayBase;
    
    public FailoverOnNetworkExceptionRetry(RetryPolicy fallbackPolicy,
        int maxFailovers) {
      this(fallbackPolicy, maxFailovers, 0, 0);
    }
    
    public FailoverOnNetworkExceptionRetry(RetryPolicy fallbackPolicy,
        int maxFailovers, long delayMillis, long maxDelayBase) {
      this.fallbackPolicy = fallbackPolicy;
      this.maxFailovers = maxFailovers;
      this.delayMillis = delayMillis;
      this.maxDelayBase = maxDelayBase;
    }

    @Override
    public RetryAction shouldRetry(Exception e, int retries,
        int failovers, boolean isMethodIdempotent) throws Exception {
      if (failovers >= maxFailovers) {
        return new RetryAction(RetryAction.RetryDecision.FAIL, 0,
            "failovers (" + failovers + ") exceeded maximum allowed ("
            + maxFailovers + ")");
      }
      
      if (e instanceof ConnectException ||
          e instanceof NoRouteToHostException ||
          e instanceof UnknownHostException ||
          e instanceof StandbyException ||
          isWrappedStandbyException(e)) {
        return new RetryAction(
            RetryAction.RetryDecision.FAILOVER_AND_RETRY,
            // retry immediately if this is our first failover, sleep otherwise
            failovers == 0 ? 0 :
                calculateExponentialTime(delayMillis, failovers, maxDelayBase));
      } else if (e instanceof SocketException ||
                 (e instanceof IOException && !(e instanceof RemoteException))) {
        if (isMethodIdempotent) {
          return RetryAction.FAILOVER_AND_RETRY;
        } else {
          return new RetryAction(RetryAction.RetryDecision.FAIL, 0,
              "the invoked method is not idempotent, and unable to determine " +
              "whether it was invoked");
        }
      } else {
        return fallbackPolicy.shouldRetry(e, retries, failovers,
            isMethodIdempotent);
      }
    }
    
  }

  /**
   * Return a value which is <code>time</code> increasing exponentially as a
   * function of <code>retries</code>, +/- 0%-50% of that value, chosen
   * randomly.
   * 
   * @param time the base amount of time to work with
   * @param retries the number of retries that have so occurred so far
   * @param cap value at which to cap the base sleep time
   * @return an amount of time to sleep
   */
  @VisibleForTesting
  public static long calculateExponentialTime(long time, int retries,
      long cap) {
    long baseTime = Math.min(time * ((long)1 << retries), cap);
    return (long) (baseTime * (RAND.nextFloat() + 0.5));
  }

  private static long calculateExponentialTime(long time, int retries) {
    return calculateExponentialTime(time, retries, Long.MAX_VALUE);
  }
  
  private static boolean isWrappedStandbyException(Exception e) {
    if (!(e instanceof RemoteException)) {
      return false;
    }
    Exception unwrapped = ((RemoteException)e).unwrapRemoteException(
        StandbyException.class);
    return unwrapped instanceof StandbyException;
  }
}
