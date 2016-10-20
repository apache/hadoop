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

package org.apache.hadoop.test;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.Time;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

/**
 * Class containing methods and associated classes to make the most of Lambda
 * expressions in Hadoop tests.
 *
 * The code has been designed from the outset to be Java-8 friendly, but still
 * be usable in Java 7.
 * In particular: support for waiting for a condition to be met.
 * This is to avoid tests having hard-coded sleeps in them.
 *
 * The code is modelled on {@code GenericTestUtils#waitFor(Supplier, int, int)},
 * but also lifts concepts from Scalatest's {@code awaitResult} and
 * its notion of pluggable retry logic (simple, backoff, maybe even things
 * with jitter: test author gets to choose).
 * The {@code intercept} method is also all credit due Scalatest, though
 * it's been extended to also support a string message check; useful when
 * checking the contents of the exception.
 */
public final class LambdaTestUtils {
  public static final Logger LOG = LoggerFactory.getLogger(LambdaTestUtils.class);

  private LambdaTestUtils() {
  }

  /**
   * This is the string included in the assertion text in
   * {@link #intercept(Class, Callable)} if
   * the closure returned a null value.
   */
  public static final String NULL_RESULT = "(null)";

  /**
   * Interface to implement for converting a timeout into some form
   * of exception to raise.
   */
  public interface TimeoutHandler {

    /**
     * Create an exception (or throw one, if desired).
     * @param timeoutMillis timeout which has arisen
     * @param caught any exception which was caught; may be null
     * @return an exception which will then be thrown
     * @throws Exception if the handler wishes to raise an exception
     * that way.
     */
    Exception evaluate(int timeoutMillis, Exception caught) throws Exception;
  }

  /**
   * Wait for a condition to be met.
   * @param check predicate to evaluate
   * @param timeoutMillis timeout in milliseconds.
   * Can be zero, in which case only one attempt is made.
   * @param retry retry escalation logic
   * @param failure handler invoked on failure; the returned exception
   * will be thrown
   * @return the number of iterations before the condition was satisfied
   * @throws Exception returned by {@code failure} on timeout
   * @throws FailFastException immediately if the evaluated operation raises it
   * @throws InterruptedException if interrupted.
   */
  public static int eventually(Callable<Boolean> check,
      int timeoutMillis,
      Callable<Integer> retry,
      TimeoutHandler failure)
      throws Exception {
    Preconditions.checkArgument(timeoutMillis >= 0,
        "timeoutMillis must be > 0");

    long endTime = Time.now() + timeoutMillis;
    Exception ex = null;
    boolean running = true;
    int iterations = 0;
    while (running) {
      iterations++;
      try {
        if (check.call()) {
          return iterations;
        }
      } catch (InterruptedException | FailFastException e) {
        throw e;
      } catch (Exception e) {
        LOG.debug("eventually() iteration {}", iterations, e);
        ex = e;
      }
      running = Time.now() < endTime;
      if (running) {
        int sleeptime = retry.call();
        if (sleeptime >= 0) {
          Thread.sleep(sleeptime);
        } else {
          running = false;
        }
      }
    }
    // timeout
    throw failure.evaluate(timeoutMillis, ex);
  }

  /**
   * Simplified {@code eventually()} clause; fixed interval
   * and {@link GenerateTimeout} used to generate the timeout.
   * @param check predicate to evaluate
   * @param timeoutMillis timeout in milliseconds.
   * Can be zero, in which case only one attempt is made.
   * @param intervalMillis interval in milliseconds between checks
   * @return the number of iterations before the condition was satisfied
   * @throws Exception returned by {@code failure} on timeout
   * @throws FailFastException immediately if the evaluated operation raises it
   * @throws InterruptedException if interrupted.
   */
  public static int eventually(Callable<Boolean> check,
      int timeoutMillis,
      int intervalMillis) throws Exception {
    return eventually(check,
        timeoutMillis,
        new FixedRetryInterval(intervalMillis),
        new GenerateTimeout());
  }

  /**
   * Await a result; exceptions are caught and, with one exception,
   * trigger a sleep and retry. This is similar of ScalaTest's
   * {@code Await.result()} operation, though that lacks the ability to
   * fail fast if the inner closure has determined that a failure condition
   * is non-recoverable.
   * @param eval expression to evaluate
   * @param timeoutMillis timeout in milliseconds.
   * Can be zero, in which case only one attempt is made.
   * @param retry retry interval generator
   * @param <T> return type
   * @return result of the first successful eval call
   * @throws Exception the last exception thrown before timeout was triggered
   * @throws FailFastException if raised -without any retry attempt.
   * @throws InterruptedException if interrupted during the sleep operation.
   */
  public static <T> T evaluate(Callable<T> eval,
      int timeoutMillis,
      Callable<Integer> retry) throws Exception {
    Preconditions.checkArgument(timeoutMillis >= 0,
        "timeoutMillis must be >= 0");
    long endTime = Time.now() + timeoutMillis;
    Exception ex;
    boolean running;
    int sleeptime;
    int iterations = 0;
    do {
      iterations++;
      try {
        return eval.call();
      } catch (InterruptedException | FailFastException e) {
        throw e;
      } catch (Exception e) {
        LOG.debug("evaluate() iteration {}", iterations, e);
        ex = e;
      }
      running = Time.now() < endTime;
      if (running && (sleeptime = retry.call()) >= 0) {
        Thread.sleep(sleeptime);
      }
    } while (running);
    // timeout. Throw the last exception raised
    throw ex;
  }

  /**
   * Simplified {@code evaluate()} clause; fixed interval.
   * @param check predicate to evaluate
   * @param timeoutMillis wait interval between check failures
   * @param intervalMillis interval in milliseconds
   * @return result of the first successful eval call
   * @throws Exception the last exception thrown before timeout was triggered
   * @throws FailFastException if raised -without any retry attempt.
   * @throws InterruptedException if interrupted during the sleep operation.
   */
  public static <T> T evaluate(Callable<T> eval,
      int timeoutMillis,
      int intervalMillis) throws Exception {
    return evaluate(eval,
        timeoutMillis,
        new FixedRetryInterval(intervalMillis));
  }

  /**
   * Intercept an exception; raise an exception if it was not raised.
   * Exceptions of the wrong class are also rethrown.
   * @param clazz class of exception; the raised exception must be this class
   * <i>or a subclass</i>.
   * @param eval expression to eval
   * @param <T> return type of expression
   * @param <E> exception class
   * @return the caught exception if it was of the expected type
   * @throws Exception any other exception raised
   * @throws AssertionError if the evaluation call didn't raise an exception.
   * The error includes the {@code toString()} value of the result, if this
   * can be determined.
   */
  public static <T, E extends Throwable> E intercept(
      Class<E> clazz,
      Callable<T> eval)
      throws Exception {
    try {
      T result = eval.call();
      throw new AssertionError("Expected an exception, got "
          + robustToString(result));
    } catch (Throwable e) {
      if (clazz.isAssignableFrom(e.getClass())) {
        return (E)e;
      } else {
        throw e;
      }
    }
  }

  /**
   * Intercept an exception; raise an exception if it was not raised.
   * Exceptions of the wrong class are also rethrown.
   * @param clazz class of exception; the raised exception must be this class
   * <i>or a subclass</i>.
   * @param contained string which must be in the {@code toString()} value
   * of the exception
   * @param eval expression to eval
   * @param <T> return type of expression
   * @param <E> exception class
   * @return the caught exception if it was of the expected type and contents
   * @throws Exception any other exception raised
   * @throws AssertionError if the evaluation call didn't raise an exception.
   * The error includes the {@code toString()} value of the result, if this
   * can be determined.
   */
  public static <T, E extends Throwable> E intercept(
      Class<E> clazz,
      String contained,
      Callable<T> eval)
      throws Exception {
    E ex = intercept(clazz, eval);
    GenericTestUtils.assertExceptionContains(contained, ex);
    return ex;
  }

  /**
   * Robust string converter for exception messages; if the {@code toString()}
   * method throws an exception then that exception is caught and logged,
   * then a simple string of the classname logged.
   * This stops a toString() failure hiding underlying problems in the code.
   * @param o object to stringify
   * @return a string for exception messages
   */
  private static String robustToString(Object o) {
    if (o == null) {
      return NULL_RESULT;
    } else {
      try {
        return o.toString();
      } catch (Exception e) {
        LOG.info("Exception calling toString()", e);
        return o.getClass().toString();
      }
    }
  }

  /**
   * Returns {@code TimeoutException} on a timeout. If
   * there was a inner class passed in, includes it as the
   * inner failure.
   */
  public static class GenerateTimeout implements TimeoutHandler {
    private final String message;

    public GenerateTimeout(String message) {
      this.message = message;
    }

    public GenerateTimeout() {
      this("timeout");
    }

    /**
     * Evaluate by creating a new Timeout Exception.
     * @param timeoutMillis timeout in millis
     * @param caught optional caught exception
     * @return TimeoutException
     */
    @Override
    public Exception evaluate(int timeoutMillis, Exception caught)
        throws Exception {
      String s = String.format("%s: after %d millis", message,
          timeoutMillis);
      String caughtText = caught != null
          ? ("; " + robustToString(caught)) : "";

      return (TimeoutException) (new TimeoutException(s + caughtText)
                                     .initCause(caught));
    }
  }

  /**
   * Retry at a fixed time period between calls.
   */
  public static class FixedRetryInterval implements Callable<Integer> {
    private final int intervalMillis;
    private int invocationCount = 0;

    public FixedRetryInterval(int intervalMillis) {
      Preconditions.checkArgument(intervalMillis > 0);
      this.intervalMillis = intervalMillis;
    }

    @Override
    public Integer call() throws Exception {
      invocationCount++;
      return intervalMillis;
    }

    public int getInvocationCount() {
      return invocationCount;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "FixedRetryInterval{");
      sb.append("interval=").append(intervalMillis);
      sb.append(", invocationCount=").append(invocationCount);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Gradually increase the sleep time by the initial interval, until
   * the limit set by {@code maxIntervalMillis} is reached.
   */
  public static class LinearRetryInterval implements Callable<Integer> {
    private final int intervalMillis;
    private final int maxIntervalMillis;
    private int current;
    private int invocationCount = 0;

    public LinearRetryInterval(int intervalMillis, int maxIntervalMillis) {
      Preconditions.checkArgument(intervalMillis > 0);
      Preconditions.checkArgument(maxIntervalMillis > 0);
      this.intervalMillis = intervalMillis;
      this.current = intervalMillis;
      this.maxIntervalMillis = maxIntervalMillis;
    }

    @Override
    public Integer call() throws Exception {
      invocationCount++;
      int last = current;
      if (last < maxIntervalMillis) {
        current += intervalMillis;
      }
      return last;
    }

    public int getInvocationCount() {
      return invocationCount;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "LinearRetryInterval{");
      sb.append("interval=").append(intervalMillis);
      sb.append(", current=").append(current);
      sb.append(", limit=").append(maxIntervalMillis);
      sb.append(", invocationCount=").append(invocationCount);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * An exception which triggers a fast exist from the
   * {@link #evaluate(Callable, int, Callable)} and
   * {@link #eventually(Callable, int, Callable, TimeoutHandler)} loops.
   */
  public static class FailFastException extends Exception {

    public FailFastException(String detailMessage) {
      super(detailMessage);
    }

    public FailFastException(String message, Throwable cause) {
      super(message, cause);
    }

    /**
     * Instantiate from a format string.
     * @param format format string
     * @param args arguments to format
     * @return an instance with the message string constructed.
     */
    public static FailFastException newInstance(String format, Object...args) {
      return new FailFastException(String.format(format, args));
    }
  }
}
