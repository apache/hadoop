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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;

/**
 * <p>
 * A collection of useful implementations of {@link RetryPolicy}.
 * </p>
 */
public class RetryPolicies {
  
  public static final Log LOG = LogFactory.getLog(RetryPolicies.class);
  
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
   * Keep trying forever with a fixed time between attempts.
   * </p>
   */
  public static final RetryPolicy retryForeverWithFixedSleep(long sleepTime,
      TimeUnit timeUnit) {
    return new RetryUpToMaximumCountWithFixedSleep(Integer.MAX_VALUE,
        sleepTime, timeUnit);
  }

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

  /**
   * A retry policy for exceptions other than RemoteException.
   */
  public static final RetryPolicy retryOtherThanRemoteException(
      RetryPolicy defaultPolicy,
      Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap) {
    return new OtherThanRemoteExceptionDependentRetry(defaultPolicy,
        exceptionToPolicyMap);
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
  
  public static final RetryPolicy failoverOnNetworkException(
      RetryPolicy fallbackPolicy, int maxFailovers, int maxRetries,
      long delayMillis, long maxDelayBase) {
    return new FailoverOnNetworkExceptionRetry(fallbackPolicy, maxFailovers,
        maxRetries, delayMillis, maxDelayBase);
  }

  static class TryOnceThenFail implements RetryPolicy {
    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      return RetryAction.FAIL;
    }
  }
  
  static class RetryForever implements RetryPolicy {
    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      return RetryAction.RETRY;
    }
  }
  
  /**
   * Retry up to maxRetries.
   * The actual sleep time of the n-th retry is f(n, sleepTime),
   * where f is a function provided by the subclass implementation.
   *
   * The object of the subclasses should be immutable;
   * otherwise, the subclass must override hashCode(), equals(..) and toString().
   */
  static abstract class RetryLimited implements RetryPolicy {
    final int maxRetries;
    final long sleepTime;
    final TimeUnit timeUnit;
    
    private String myString;

    RetryLimited(int maxRetries, long sleepTime, TimeUnit timeUnit) {
      if (maxRetries < 0) {
        throw new IllegalArgumentException("maxRetries = " + maxRetries+" < 0");
      }
      if (sleepTime < 0) {
        throw new IllegalArgumentException("sleepTime = " + sleepTime + " < 0");
      }

      this.maxRetries = maxRetries;
      this.sleepTime = sleepTime;
      this.timeUnit = timeUnit;
    }

    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      if (retries >= maxRetries) {
        return RetryAction.FAIL;
      }
      return new RetryAction(RetryAction.RetryDecision.RETRY,
          timeUnit.toMillis(calculateSleepTime(retries)));
    }
    
    protected abstract long calculateSleepTime(int retries);
    
    @Override
    public int hashCode() {
      return toString().hashCode();
    }
    
    @Override
    public boolean equals(final Object that) {
      if (this == that) {
        return true;
      } else if (that == null || this.getClass() != that.getClass()) {
        return false;
      }
      return this.toString().equals(that.toString());
    }

    @Override
    public String toString() {
      if (myString == null) {
        myString = getClass().getSimpleName() + "(maxRetries=" + maxRetries
            + ", sleepTime=" + sleepTime + " " + timeUnit + ")";
      }
      return myString;
    }
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
  
  /**
   * Given pairs of number of retries and sleep time (n0, t0), (n1, t1), ...,
   * the first n0 retries sleep t0 milliseconds on average,
   * the following n1 retries sleep t1 milliseconds on average, and so on.
   * 
   * For all the sleep, the actual sleep time is randomly uniform distributed
   * in the close interval [0.5t, 1.5t], where t is the sleep time specified.
   *
   * The objects of this class are immutable.
   */
  public static class MultipleLinearRandomRetry implements RetryPolicy {
    /** Pairs of numRetries and sleepSeconds */
    public static class Pair {
      final int numRetries;
      final int sleepMillis;
      
      public Pair(final int numRetries, final int sleepMillis) {
        if (numRetries < 0) {
          throw new IllegalArgumentException("numRetries = " + numRetries+" < 0");
        }
        if (sleepMillis < 0) {
          throw new IllegalArgumentException("sleepMillis = " + sleepMillis + " < 0");
        }

        this.numRetries = numRetries;
        this.sleepMillis = sleepMillis;
      }
      
      @Override
      public String toString() {
        return numRetries + "x" + sleepMillis + "ms";
      }
    }

    private final List<Pair> pairs;
    private String myString;

    public MultipleLinearRandomRetry(List<Pair> pairs) {
      if (pairs == null || pairs.isEmpty()) {
        throw new IllegalArgumentException("pairs must be neither null nor empty.");
      }
      this.pairs = Collections.unmodifiableList(pairs);
    }

    @Override
    public RetryAction shouldRetry(Exception e, int curRetry, int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      final Pair p = searchPair(curRetry);
      if (p == null) {
        //no more retries.
        return RetryAction.FAIL;
      }

      //calculate sleep time and return.
      // ensure 0.5 <= ratio <=1.5
      final double ratio = ThreadLocalRandom.current().nextDouble() + 0.5;
      final long sleepTime = Math.round(p.sleepMillis * ratio);
      return new RetryAction(RetryAction.RetryDecision.RETRY, sleepTime);
    }

    /**
     * Given the current number of retry, search the corresponding pair.
     * @return the corresponding pair,
     *   or null if the current number of retry > maximum number of retry. 
     */
    private Pair searchPair(int curRetry) {
      int i = 0;
      for(; i < pairs.size() && curRetry > pairs.get(i).numRetries; i++) {
        curRetry -= pairs.get(i).numRetries;
      }
      return i == pairs.size()? null: pairs.get(i);
    }
    
    @Override
    public int hashCode() {
      return toString().hashCode();
    }
    
    @Override
    public boolean equals(final Object that) {
      if (this == that) {
        return true;
      } else if (that == null || this.getClass() != that.getClass()) {
        return false;
      }
      return this.toString().equals(that.toString());
    }

    @Override
    public String toString() {
      if (myString == null) {
        myString = getClass().getSimpleName() + pairs;
      }
      return myString;
    }

    /**
     * Parse the given string as a MultipleLinearRandomRetry object.
     * The format of the string is "t_1, n_1, t_2, n_2, ...",
     * where t_i and n_i are the i-th pair of sleep time and number of retries.
     * Note that the white spaces in the string are ignored.
     *
     * @return the parsed object, or null if the parsing fails.
     */
    public static MultipleLinearRandomRetry parseCommaSeparatedString(String s) {
      final String[] elements = s.split(",");
      if (elements.length == 0) {
        LOG.warn("Illegal value: there is no element in \"" + s + "\".");
        return null;
      }
      if (elements.length % 2 != 0) {
        LOG.warn("Illegal value: the number of elements in \"" + s + "\" is "
            + elements.length + " but an even number of elements is expected.");
        return null;
      }

      final List<RetryPolicies.MultipleLinearRandomRetry.Pair> pairs
          = new ArrayList<RetryPolicies.MultipleLinearRandomRetry.Pair>();
   
      for(int i = 0; i < elements.length; ) {
        //parse the i-th sleep-time
        final int sleep = parsePositiveInt(elements, i++, s);
        if (sleep == -1) {
          return null; //parse fails
        }

        //parse the i-th number-of-retries
        final int retries = parsePositiveInt(elements, i++, s);
        if (retries == -1) {
          return null; //parse fails
        }

        pairs.add(new RetryPolicies.MultipleLinearRandomRetry.Pair(retries, sleep));
      }
      return new RetryPolicies.MultipleLinearRandomRetry(pairs);
    }

    /**
     * Parse the i-th element as an integer.
     * @return -1 if the parsing fails or the parsed value <= 0;
     *   otherwise, return the parsed value.
     */
    private static int parsePositiveInt(final String[] elements,
        final int i, final String originalString) {
      final String s = elements[i].trim();
      final int n;
      try {
        n = Integer.parseInt(s);
      } catch(NumberFormatException nfe) {
        LOG.warn("Failed to parse \"" + s + "\", which is the index " + i
            + " element in \"" + originalString + "\"", nfe);
        return -1;
      }

      if (n <= 0) {
        LOG.warn("The value " + n + " <= 0: it is parsed from the string \""
            + s + "\" which is the index " + i + " element in \""
            + originalString + "\"");
        return -1;
      }
      return n;
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

    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      RetryPolicy policy = exceptionToPolicyMap.get(e.getClass());
      if (policy == null) {
        policy = defaultPolicy;
      }
      return policy.shouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
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

    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      RetryPolicy policy = null;
      if (e instanceof RemoteException) {
        policy = exceptionNameToPolicyMap.get(
            ((RemoteException) e).getClassName());
      }
      if (policy == null) {
        policy = defaultPolicy;
      }
      return policy.shouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
    }
  }

  static class OtherThanRemoteExceptionDependentRetry implements RetryPolicy {

    private RetryPolicy defaultPolicy;
    private Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap;

    public OtherThanRemoteExceptionDependentRetry(RetryPolicy defaultPolicy,
        Map<Class<? extends Exception>,
        RetryPolicy> exceptionToPolicyMap) {
      this.defaultPolicy = defaultPolicy;
      this.exceptionToPolicyMap = exceptionToPolicyMap;
    }

    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers,
        boolean isIdempotentOrAtMostOnce) throws Exception {
      RetryPolicy policy = null;
      // ignore Remote Exception
      if (e instanceof RemoteException) {
        // do nothing
      } else {
        policy = exceptionToPolicyMap.get(e.getClass());
      }
      if (policy == null) {
        policy = defaultPolicy;
      }
      return policy.shouldRetry(
          e, retries, failovers, isIdempotentOrAtMostOnce);
    }
  }

  static class ExponentialBackoffRetry extends RetryLimited {
    
    public ExponentialBackoffRetry(
        int maxRetries, long sleepTime, TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);

      if (maxRetries < 0) {
        throw new IllegalArgumentException("maxRetries = " + maxRetries + " < 0");
      } else if (maxRetries >= Long.SIZE - 1) {
        //calculateSleepTime may overflow. 
        throw new IllegalArgumentException("maxRetries = " + maxRetries
            + " >= " + (Long.SIZE - 1));
      }
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
    private int maxRetries;
    private long delayMillis;
    private long maxDelayBase;
    
    public FailoverOnNetworkExceptionRetry(RetryPolicy fallbackPolicy,
        int maxFailovers) {
      this(fallbackPolicy, maxFailovers, 0, 0, 0);
    }
    
    public FailoverOnNetworkExceptionRetry(RetryPolicy fallbackPolicy,
        int maxFailovers, long delayMillis, long maxDelayBase) {
      this(fallbackPolicy, maxFailovers, 0, delayMillis, maxDelayBase);
    }
    
    public FailoverOnNetworkExceptionRetry(RetryPolicy fallbackPolicy,
        int maxFailovers, int maxRetries, long delayMillis, long maxDelayBase) {
      this.fallbackPolicy = fallbackPolicy;
      this.maxFailovers = maxFailovers;
      this.maxRetries = maxRetries;
      this.delayMillis = delayMillis;
      this.maxDelayBase = maxDelayBase;
    }

    /**
     * @return 0 if this is our first failover/retry (i.e., retry immediately),
     *         sleep exponentially otherwise
     */
    private long getFailoverOrRetrySleepTime(int times) {
      return times == 0 ? 0 : 
        calculateExponentialTime(delayMillis, times, maxDelayBase);
    }
    
    @Override
    public RetryAction shouldRetry(Exception e, int retries,
        int failovers, boolean isIdempotentOrAtMostOnce) throws Exception {
      if (failovers >= maxFailovers) {
        return new RetryAction(RetryAction.RetryDecision.FAIL, 0,
            "failovers (" + failovers + ") exceeded maximum allowed ("
            + maxFailovers + ")");
      }
      if (retries - failovers > maxRetries) {
        return new RetryAction(RetryAction.RetryDecision.FAIL, 0, "retries ("
            + retries + ") exceeded maximum allowed (" + maxRetries + ")");
      }
      
      if (e instanceof ConnectException ||
          e instanceof NoRouteToHostException ||
          e instanceof UnknownHostException ||
          e instanceof StandbyException ||
          e instanceof ConnectTimeoutException ||
          isWrappedStandbyException(e)) {
        return new RetryAction(RetryAction.RetryDecision.FAILOVER_AND_RETRY,
            getFailoverOrRetrySleepTime(failovers));
      } else if (e instanceof RetriableException
          || getWrappedRetriableException(e) != null) {
        // RetriableException or RetriableException wrapped 
        return new RetryAction(RetryAction.RetryDecision.RETRY,
              getFailoverOrRetrySleepTime(retries));
      } else if (e instanceof InvalidToken) {
        return new RetryAction(RetryAction.RetryDecision.FAIL, 0,
            "Invalid or Cancelled Token");
      } else if (e instanceof SocketException
          || (e instanceof IOException && !(e instanceof RemoteException))) {
        if (isIdempotentOrAtMostOnce) {
          return RetryAction.FAILOVER_AND_RETRY;
        } else {
          return new RetryAction(RetryAction.RetryDecision.FAIL, 0,
              "the invoked method is not idempotent, and unable to determine "
                  + "whether it was invoked");
        }
      } else {
          return fallbackPolicy.shouldRetry(e, retries, failovers,
              isIdempotentOrAtMostOnce);
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
  private static long calculateExponentialTime(long time, int retries,
      long cap) {
    long baseTime = Math.min(time * (1L << retries), cap);
    return (long) (baseTime * (ThreadLocalRandom.current().nextDouble() + 0.5));
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
  
  static RetriableException getWrappedRetriableException(Exception e) {
    if (!(e instanceof RemoteException)) {
      return null;
    }
    Exception unwrapped = ((RemoteException)e).unwrapRemoteException(
        RetriableException.class);
    return unwrapped instanceof RetriableException ? 
        (RetriableException) unwrapped : null;
  }
}
