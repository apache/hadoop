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

import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.test.LambdaTestUtils.*;
import static org.apache.hadoop.test.GenericTestUtils.*;

/**
 * Test the logic in {@link LambdaTestUtils}.
 * This test suite includes Java 8 and Java 7 code; the Java 8 code exists
 * to verify that the API is easily used with Lambda expressions.
 */
public class TestLambdaTestUtils extends Assert {

  public static final int INTERVAL = 10;
  public static final int TIMEOUT = 50;
  private FixedRetryInterval retry = new FixedRetryInterval(INTERVAL);

  /**
   * Always evaluates to true.
   */
  public static final Callable<Boolean> ALWAYS_TRUE =
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return true;
        }
      };

  /**
   * Always evaluates to false.
   */
  public static final Callable<Boolean> ALWAYS_FALSE =
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return false;
        }
      };

  /**
   * Text in the raised FNFE.
   */
  public static final String MISSING = "not found";

  /**
   * A predicate that always throws a FileNotFoundException.
   */
  public static final Callable<Boolean> ALWAYS_FNFE =
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          throw new FileNotFoundException(MISSING);
        }
      };

  /**
   * reusable timeout handler.
   */
  public static final GenerateTimeout
      TIMEOUT_FAILURE_HANDLER = new GenerateTimeout();

  /**
   * Always evaluates to 3L.
   */
  public static final Callable<Long> EVAL_3L = new Callable<Long>() {
    @Override
    public Long call() throws Exception {
      return 3L;
    }
  };

  /**
   * Always raises a {@code FileNotFoundException}.
   */
  public static final Callable<Long> EVAL_FNFE = new Callable<Long>() {
    @Override
    public Long call() throws Exception {
      throw new FileNotFoundException(MISSING);
    }
  };

  /**
   * Assert the retry count is as expected.
   * @param expected expected value
   */
  protected void assertRetryCount(int expected) {
    assertEquals(retry.toString(), expected, retry.getInvocationCount());
  }

  /**
   * Assert the retry count is as expected.
   * @param minCount minimum value
   */
  protected void assertMinRetryCount(int minCount) {
    assertTrue("retry count of " + retry + " is not >= " + minCount,
        minCount <= retry.getInvocationCount());
  }

  @Test
  public void testAwaitAlwaysTrue() throws Throwable {
    await(TIMEOUT,
        ALWAYS_TRUE,
        new FixedRetryInterval(INTERVAL),
        TIMEOUT_FAILURE_HANDLER);
  }

  @Test
  public void testAwaitAlwaysFalse() throws Throwable {
    try {
      await(TIMEOUT,
          ALWAYS_FALSE,
          retry,
          TIMEOUT_FAILURE_HANDLER);
      fail("should not have got here");
    } catch (TimeoutException e) {
      assertMinRetryCount(1);
    }
  }

  @Test
  public void testAwaitLinearRetry() throws Throwable {
    ProportionalRetryInterval linearRetry =
        new ProportionalRetryInterval(INTERVAL * 2, TIMEOUT * 2);
    try {
      await(TIMEOUT,
          ALWAYS_FALSE,
          linearRetry,
          TIMEOUT_FAILURE_HANDLER);
      fail("should not have got here");
    } catch (TimeoutException e) {
      assertEquals(linearRetry.toString(),
          2, linearRetry.getInvocationCount());
    }
  }

  @Test
  public void testAwaitFNFE() throws Throwable {
    try {
      await(TIMEOUT,
          ALWAYS_FNFE,
          retry,
          TIMEOUT_FAILURE_HANDLER);
      fail("should not have got here");
    } catch (TimeoutException e) {
      // inner clause is included
      assertTrue(retry.getInvocationCount() > 0);
      assertTrue(e.getCause() instanceof FileNotFoundException);
      assertExceptionContains(MISSING, e);
    }
  }

  @Test
  public void testRetryInterval() throws Throwable {
    ProportionalRetryInterval interval =
        new ProportionalRetryInterval(200, 1000);
    assertEquals(200, (int) interval.call());
    assertEquals(400, (int) interval.call());
    assertEquals(600, (int) interval.call());
    assertEquals(800, (int) interval.call());
    assertEquals(1000, (int) interval.call());
    assertEquals(1000, (int) interval.call());
    assertEquals(1000, (int) interval.call());
  }

  @Test
  public void testInterceptSuccess() throws Throwable {
    IOException ioe = intercept(IOException.class, ALWAYS_FNFE);
    assertExceptionContains(MISSING, ioe);
  }

  @Test
  public void testInterceptContains() throws Throwable {
    intercept(IOException.class, MISSING, ALWAYS_FNFE);
  }

  @Test
  public void testInterceptContainsWrongString() throws Throwable {
    try {
      FileNotFoundException e =
          intercept(FileNotFoundException.class, "404", ALWAYS_FNFE);
      assertNotNull(e);
      throw e;
    } catch (AssertionError expected) {
      assertExceptionContains(MISSING, expected);
    }
  }

  @Test
  public void testInterceptVoidCallable() throws Throwable {
    intercept(AssertionError.class,
        NULL_RESULT,
        new Callable<IOException>() {
          @Override
          public IOException call() throws Exception {
            return intercept(IOException.class,
                new Callable<Void>() {
                  @Override
                  public Void call() throws Exception {
                    return null;
                  }
                });
          }
        });
  }

  @Test
  public void testEventually() throws Throwable {
    long result = eventually(TIMEOUT, EVAL_3L, retry);
    assertEquals(3, result);
    assertEquals(0, retry.getInvocationCount());
  }

  @Test
  public void testEventuallyFailuresRetry() throws Throwable {
    try {
      eventually(TIMEOUT, EVAL_FNFE, retry);
      fail("should not have got here");
    } catch (IOException expected) {
      // expected
      assertMinRetryCount(1);
    }
  }

}
