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
 * The code has been designed from the outset to be Java-8 friendly, but
 * to still be usable in Java 7.
 *
 * The code is modelled on {@code GenericTestUtils#waitFor(Supplier, int, int)},
 * but also lifts concepts from Scalatest's {@code awaitResult} and
 * its notion of pluggable retry logic (simple, backoff, maybe even things
 * with jitter: test author gets to choose).
 * The {@link #intercept(Class, Callable)} method is also all credit due
 * Scalatest, though it's been extended to also support a string message
 * check; useful when checking the contents of the exception.
 */
public final class LambdaTestUtils {
  public static final Logger LOG =
      LoggerFactory.getLogger(LambdaTestUtils.class);

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
   * Wait for a condition to be met, with a retry policy returning the
   * sleep time before the next attempt is made. If, at the end
   * of the timeout period, the condition is still false (or failing with
   * an exception), the timeout handler is invoked, passing in the timeout
   * and any exception raised in the last invocation. The exception returned
   * by this timeout handler is then rethrown.
   * <p>
   * Example: Wait 30s for a condition to be met, with a sleep of 30s
   * between each probe.
   * If the operation is failing, then, after 30s, the timeout handler
   * is called. This returns the exception passed in (if any),
   * or generates a new one.
   * <pre>
   * await(
   *   30 * 1000,
   *   () -> { return 0 == filesystem.listFiles(new Path("/")).length); },
   *   () -> 500),
   *   (timeout, ex) -> ex != null ? ex : new TimeoutException("timeout"));
   * </pre>
   *
   * @param timeoutMillis timeout in milliseconds.
   * Can be zero, in which case only one attempt is made.
   * @param check predicate to evaluate
   * @param retry retry escalation logic
   * @param timeoutHandler handler invoked on timeout;
   * the returned exception will be thrown
   * @return the number of iterations before the condition was satisfied
   * @throws Exception the exception returned by {@code timeoutHandler} on
   * timeout
   * @throws FailFastException immediately if the evaluated operation raises it
   * @throws InterruptedException if interrupted.
   */
  public static int await(int timeoutMillis,
      Callable<Boolean> check,
      Callable<Integer> retry,
      TimeoutHandler timeoutHandler)
      throws Exception {
    Preconditions.checkArgument(timeoutMillis >= 0,
        "timeoutMillis must be >= 0");
    Preconditions.checkNotNull(timeoutHandler);

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
        // the probe failed but did not raise an exception. Reset any
        // exception raised by a previous probe failure.
        ex = null;
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
    Exception evaluate = timeoutHandler.evaluate(timeoutMillis, ex);
    if (evaluate == null) {
      // bad timeout handler logic; fall back to GenerateTimeout so the
      // underlying problem isn't lost.
      LOG.error("timeout handler {} did not throw an exception ",
          timeoutHandler);
      evaluate = new GenerateTimeout().evaluate(timeoutMillis, ex);
    }
    throw evaluate;
  }

  /**
   * Simplified {@link #await(int, Callable, Callable, TimeoutHandler)}
   * operation with a fixed interval
   * and {@link GenerateTimeout} handler to generate a {@code TimeoutException}.
   * <p>
   * Example: await for probe to succeed:
   * <pre>
   * await(
   *   30 * 1000, 500,
   *   () -> { return 0 == filesystem.listFiles(new Path("/")).length); });
   * </pre>
   *
   * @param timeoutMillis timeout in milliseconds.
   * Can be zero, in which case only one attempt is made.
   * @param intervalMillis interval in milliseconds between checks
   * @param check predicate to evaluate
   * @return the number of iterations before the condition was satisfied
   * @throws Exception returned by {@code failure} on timeout
   * @throws FailFastException immediately if the evaluated operation raises it
   * @throws InterruptedException if interrupted.
   */
  public static int await(int timeoutMillis,
      int intervalMillis,
      Callable<Boolean> check) throws Exception {
    return await(timeoutMillis, check,
        new FixedRetryInterval(intervalMillis),
        new GenerateTimeout());
  }

  /**
   * Repeatedly execute a closure until it returns a value rather than
   * raise an exception.
   * Exceptions are caught and, with one exception,
   * trigger a sleep and retry. This is similar of ScalaTest's
   * {@code eventually(timeout, closure)} operation, though that lacks
   * the ability to fail fast if the inner closure has determined that
   * a failure condition is non-recoverable.
   * <p>
   * Example: spin until an the number of files in a filesystem is non-zero,
   * returning the files found.
   * The sleep interval backs off by 500 ms each iteration to a maximum of 5s.
   * <pre>
   * FileStatus[] files = eventually( 30 * 1000,
   *   () -> {
   *     FileStatus[] f = filesystem.listFiles(new Path("/"));
   *     assertEquals(0, f.length);
   *     return f;
   *   },
   *   new ProportionalRetryInterval(500, 5000));
   * </pre>
   * This allows for a fast exit, yet reduces probe frequency over time.
   *
   * @param <T> return type
   * @param timeoutMillis timeout in milliseconds.
   * Can be zero, in which case only one attempt is made before failing.
   * @param eval expression to evaluate
   * @param retry retry interval generator
   * @return result of the first successful eval call
   * @throws Exception the last exception thrown before timeout was triggered
   * @throws FailFastException if raised -without any retry attempt.
   * @throws InterruptedException if interrupted during the sleep operation.
   */
  public static <T> T eventually(int timeoutMillis,
      Callable<T> eval,
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
        // these two exceptions trigger an immediate exit
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
   * Variant of {@link #eventually(int, Callable, Callable)} method for
   * void lambda expressions.
   * @param timeoutMillis timeout in milliseconds.
   * Can be zero, in which case only one attempt is made before failing.
   * @param eval expression to evaluate
   * @param retry retry interval generator
   * @throws Exception the last exception thrown before timeout was triggered
   * @throws FailFastException if raised -without any retry attempt.
   * @throws InterruptedException if interrupted during the sleep operation.
   */
  public static void eventually(int timeoutMillis,
      VoidCallable eval,
      Callable<Integer> retry) throws Exception {
    eventually(timeoutMillis, new VoidCaller(eval), retry);
  }

  /**
   * Simplified {@link #eventually(int, Callable, Callable)} method
   * with a fixed interval.
   * <p>
   * Example: wait 30s until an assertion holds, sleeping 1s between each
   * check.
   * <pre>
   * eventually( 30 * 1000, 1000,
   *   () -> { assertEquals(0, filesystem.listFiles(new Path("/")).length); }
   * );
   * </pre>
   *
   * @param timeoutMillis timeout in milliseconds.
   * Can be zero, in which case only one attempt is made before failing.
   * @param intervalMillis interval in milliseconds
   * @param eval expression to evaluate
   * @return result of the first successful invocation of {@code eval()}
   * @throws Exception the last exception thrown before timeout was triggered
   * @throws FailFastException if raised -without any retry attempt.
   * @throws InterruptedException if interrupted during the sleep operation.
   */
  public static <T> T eventually(int timeoutMillis,
      int intervalMillis,
      Callable<T> eval) throws Exception {
    return eventually(timeoutMillis, eval,
        new FixedRetryInterval(intervalMillis));
  }

  /**
   /**
   * Variant of {@link #eventually(int, int, Callable)} method for
   * void lambda expressions.
   * @param timeoutMillis timeout in milliseconds.
   * Can be zero, in which case only one attempt is made before failing.
   * @param intervalMillis interval in milliseconds
   * @param eval expression to evaluate
   * @throws Exception the last exception thrown before timeout was triggered
   * @throws FailFastException if raised -without any retry attempt.
   * @throws InterruptedException if interrupted during the sleep operation.
   */
  public static void eventually(int timeoutMillis,
      int intervalMillis,
      VoidCallable eval) throws Exception {
    eventually(timeoutMillis, eval,
        new FixedRetryInterval(intervalMillis));
  }

  /**
   * Intercept an exception; throw an {@code AssertionError} if one not raised.
   * The caught exception is rethrown if it is of the wrong class or
   * does not contain the text defined in {@code contained}.
   * <p>
   * Example: expect deleting a nonexistent file to raise a
   * {@code FileNotFoundException}.
   * <pre>
   * FileNotFoundException ioe = intercept(FileNotFoundException.class,
   *   () -> {
   *     filesystem.delete(new Path("/missing"), false);
   *   });
   * </pre>
   *
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
  @SuppressWarnings("unchecked")
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
      }
      throw e;
    }
  }

  /**
   * Variant of {@link #intercept(Class, Callable)} to simplify void
   * invocations.
   * @param clazz class of exception; the raised exception must be this class
   * <i>or a subclass</i>.
   * @param eval expression to eval
   * @param <E> exception class
   * @return the caught exception if it was of the expected type
   * @throws Exception any other exception raised
   * @throws AssertionError if the evaluation call didn't raise an exception.
   */
  public static <E extends Throwable> E intercept(
      Class<E> clazz,
      VoidCallable eval)
      throws Exception {
    try {
      eval.call();
      throw new AssertionError("Expected an exception");
    } catch (Throwable e) {
      if (clazz.isAssignableFrom(e.getClass())) {
        return (E)e;
      }
      throw e;
    }
  }

  /**
   * Intercept an exception; throw an {@code AssertionError} if one not raised.
   * The caught exception is rethrown if it is of the wrong class or
   * does not contain the text defined in {@code contained}.
   * <p>
   * Example: expect deleting a nonexistent file to raise a
   * {@code FileNotFoundException} with the {@code toString()} value
   * containing the text {@code "missing"}.
   * <pre>
   * FileNotFoundException ioe = intercept(FileNotFoundException.class,
   *   "missing",
   *   () -> {
   *     filesystem.delete(new Path("/missing"), false);
   *   });
   * </pre>
   *
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
   * @see GenericTestUtils#assertExceptionContains(String, Throwable)
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
   * Variant of {@link #intercept(Class, Callable)} to simplify void
   * invocations.
   * @param clazz class of exception; the raised exception must be this class
   * <i>or a subclass</i>.
   * @param contained string which must be in the {@code toString()} value
   * of the exception
   * @param eval expression to eval
   * @param <E> exception class
   * @return the caught exception if it was of the expected type
   * @throws Exception any other exception raised
   * @throws AssertionError if the evaluation call didn't raise an exception.
   */
  public static <E extends Throwable> E intercept(
      Class<E> clazz,
      String contained,
      VoidCallable eval)
      throws Exception {
    E ex = intercept(clazz, eval);
    GenericTestUtils.assertExceptionContains(contained, ex);
    return ex;
  }

  /**
   * Robust string converter for exception messages; if the {@code toString()}
   * method throws an exception then that exception is caught and logged,
   * then a simple string of the classname logged.
   * This stops a {@code toString()} failure hiding underlying problems.
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
     * Evaluate operation creates a new {@code TimeoutException}.
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
  public static class ProportionalRetryInterval implements Callable<Integer> {
    private final int intervalMillis;
    private final int maxIntervalMillis;
    private int current;
    private int invocationCount = 0;

    public ProportionalRetryInterval(int intervalMillis,
        int maxIntervalMillis) {
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
          "ProportionalRetryInterval{");
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
   * {@link #eventually(int, Callable, Callable)} and
   * {@link #await(int, Callable, Callable, TimeoutHandler)} loops.
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

  /**
   * A simple interface for lambdas, which returns nothing; this exists
   * to simplify lambda tests on operations with no return value.
   */
  public interface VoidCallable {
    void call() throws Exception;
  }

  /**
   * Bridge class to make {@link VoidCallable} something to use in anything
   * which takes an {@link Callable}.
   */
  public static class VoidCaller implements Callable<Void> {
    private final VoidCallable callback;

    public VoidCaller(VoidCallable callback) {
      this.callback = callback;
    }

    @Override
    public Void call() throws Exception {
      callback.call();
      return null;
    }
  }

}
