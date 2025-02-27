/**
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
package org.apache.hadoop.hdfs.server.federation.router.async.utils;

import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The TestAsyncUtil class provides a suite of test cases for the
 * asynchronous utility class AsyncUtil. It utilizes the JUnit testing
 * framework to verify that asynchronous operations are performed as
 * expected.
 *
 * <p>
 * This class contains multiple test methods designed to test various
 * asynchronous operation scenarios, including:
 * <ul>
 *     <li>testApply - Tests the asynchronous application of a method.</li>
 *     <li>testApplyException - Tests exception handling in
 *     asynchronous methods.</li>
 *     <li>testApplyThenApplyMethod - Tests the chaining of
 *     asynchronous method calls.</li>
 *     <li>testCatchThenApplyMethod - Tests the invocation of
 *     asynchronous methods after exception catching.</li>
 *     <li>testForEach - Tests asynchronous iteration operations.</li>
 *     <li>testForEachBreak - Tests asynchronous iteration with break
 *     conditions.</li>
 *     <li>testForEachBreakByException - Tests the interruption of
 *     asynchronous iteration due to exceptions.</li>
 * </ul>
 * </p>
 *
 * The tests cover both synchronous (Sync) and asynchronous (Async)
 * configurations to ensure consistent behavior under different
 * execution modes.
 *
 * @see AsyncUtil
 * @see BaseClass
 * @see SyncClass
 * @see AsyncClass
 */
public class TestAsyncUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestAsyncUtil.class);
  private static final long TIME_CONSUMING = 100;
  private BaseClass baseClass;
  private boolean enableAsync;

  public enum ExecutionMode {
    SYNC,
    ASYNC
  }

  @Before
  public void setUp(ExecutionMode mode) {
    if (mode.equals(ExecutionMode.ASYNC)) {
      baseClass = new AsyncClass(TIME_CONSUMING);
      enableAsync = true;
    } else {
      baseClass = new SyncClass(TIME_CONSUMING);
    }
  }

  @After
  public void after() {
    baseClass = null;
    enableAsync = false;
  }

  @EnumSource(ExecutionMode.class)
  @ParameterizedTest
  public void testApply(ExecutionMode mode)
      throws Exception {
    setUp(mode);
    long start = Time.monotonicNow();
    String result = baseClass.applyMethod(1);
    long cost = Time.monotonicNow() - start;
    LOG.info("[{}] main thread cost: {} ms", mode, cost);
    checkResult("applyMethod[1]", result, TIME_CONSUMING, cost);
  }

  @EnumSource(ExecutionMode.class)
  @ParameterizedTest
  public void testApplyException(ExecutionMode mode) throws Exception {
    setUp(mode);
    checkException(
        () -> baseClass.applyMethod(2, true),
        IOException.class, "input 2 exception");

    checkException(
        () -> baseClass.applyMethod(3, true),
        RuntimeException.class, "input 3 exception");
  }

  @EnumSource(ExecutionMode.class)
  @ParameterizedTest
  public void testExceptionMethod(ExecutionMode mode) throws Exception {
    setUp(mode);
    checkException(
        () -> baseClass.exceptionMethod(2),
        IOException.class, "input 2 exception");

    checkException(
        () -> baseClass.exceptionMethod(3),
        RuntimeException.class, "input 3 exception");

    long start = Time.monotonicNow();
    String result = baseClass.exceptionMethod(1);
    long cost = Time.monotonicNow() - start;
    LOG.info("[{}] main thread cost: {} ms", mode, cost);
    checkResult("applyMethod[1]", result, TIME_CONSUMING, cost);
  }

  @EnumSource(ExecutionMode.class)
  @ParameterizedTest
  public void testApplyThenApplyMethod(ExecutionMode mode) throws Exception {
    setUp(mode);
    long start = Time.monotonicNow();
    String result = baseClass.applyThenApplyMethod(1);
    long cost = Time.monotonicNow() - start;
    checkResult("[2]", result, TIME_CONSUMING, cost);
    LOG.info("[{}] main thread cost: {} ms", mode, cost);

    start = Time.monotonicNow();
    result = baseClass.applyThenApplyMethod(3);
    cost = Time.monotonicNow() - start;
    checkResult("[3]", result, TIME_CONSUMING, cost);
    LOG.info("[{}] main thread cost: {} ms", mode, cost);
  }

  @EnumSource(ExecutionMode.class)
  @ParameterizedTest
  public void testCatchThenApplyMethod(ExecutionMode mode) throws Exception {
    setUp(mode);
    long start = Time.monotonicNow();
    String result = baseClass.applyCatchThenApplyMethod(2);
    long cost = Time.monotonicNow() - start;
    checkResult("applyMethod[1]", result, TIME_CONSUMING, cost);
    LOG.info("[{}] main thread cost: {} ms", mode, cost);

    start = Time.monotonicNow();
    result = baseClass.applyCatchThenApplyMethod(0);
    cost = Time.monotonicNow() - start;
    checkResult("[0]", result, TIME_CONSUMING, cost);
    LOG.info("[{}] main thread cost: {} ms", mode, cost);
  }

  @EnumSource(ExecutionMode.class)
  @ParameterizedTest
  public void testCatchFinallyMethod(ExecutionMode mode) throws Exception {
    setUp(mode);
    List<String> resource = new ArrayList<>();
    resource.add("resource1");
    checkException(
        () -> baseClass.applyCatchFinallyMethod(2, resource),
        IOException.class, "input 2 exception");
    assertTrue(resource.size() == 0);

    long start = Time.monotonicNow();
    String result = baseClass.applyCatchFinallyMethod(0, resource);
    long cost = Time.monotonicNow() - start;
    checkResult("[0]", result, TIME_CONSUMING, cost);
    assertTrue(resource.size() == 0);
    LOG.info("[{}] main thread cost: {} ms", mode, cost);
  }

  @EnumSource(ExecutionMode.class)
  @ParameterizedTest
  public void testForEach(ExecutionMode mode) throws Exception {
    setUp(mode);
    long start = Time.monotonicNow();
    String result = baseClass.forEachMethod(Arrays.asList(1, 2, 3));
    long cost = Time.monotonicNow() - start;
    LOG.info("[{}] main thread cost: {} ms", mode, cost);
    checkResult("forEach[1],forEach[2],forEach[3],", result,
        TIME_CONSUMING, cost);
  }

  @EnumSource(ExecutionMode.class)
  @ParameterizedTest
  public void testForEachBreak(ExecutionMode mode) throws Exception {
    setUp(mode);
    long start = Time.monotonicNow();
    String result = baseClass.forEachBreakMethod(Arrays.asList(1, 2, 3));
    long cost = Time.monotonicNow() - start;
    LOG.info("[{}] main thread cost: {} ms", mode, cost);
    checkResult("forEach[1],", result, TIME_CONSUMING, cost);
  }

  @EnumSource(ExecutionMode.class)
  @ParameterizedTest
  public void testForEachBreakByException(ExecutionMode mode)
      throws Exception {
    setUp(mode);
    long start = Time.monotonicNow();
    String result = baseClass.forEachBreakByExceptionMethod(Arrays.asList(1, 2, 3));
    long cost = Time.monotonicNow() - start;
    LOG.info("[{}] main thread cost: {} ms", mode, cost);
    checkResult("forEach[1],java.io.IOException: input 2 exception,",
        result, TIME_CONSUMING, cost);
  }

  @EnumSource(ExecutionMode.class)
  @ParameterizedTest
  public void testCurrentMethod(ExecutionMode mode)
      throws Exception {
    setUp(mode);
    long start = Time.monotonicNow();
    String result = baseClass.currentMethod(Arrays.asList(1, 2, 3));
    long cost = Time.monotonicNow() - start;
    LOG.info("[{}] main thread cost: {} ms", mode, cost);
    checkResult("[1],java.io.IOException: input 2 exception," +
            "java.lang.RuntimeException: input 3 exception,",
        result, TIME_CONSUMING, cost);
  }

  private void checkResult(
      String result, String actualResult, long cost, long actualCost)
      throws Exception {
    if (enableAsync) {
      Assertions.assertNull(actualResult);
      actualResult = AsyncUtil.syncReturn(String.class);
      assertNotNull(actualResult);
      assertTrue(actualCost < cost);
    } else {
      assertFalse(actualCost < cost);
    }
    assertEquals(result, actualResult);
  }

  private < E extends Throwable> void checkException(
      Callable<String> eval, Class<E> clazz, String contained) throws Exception {
    if (enableAsync) {
      LambdaTestUtils.intercept(clazz, contained,
          () -> {
            eval.call();
            return AsyncUtil.syncReturn(String.class);
          });
    } else {
      LambdaTestUtils.intercept(clazz, contained, () -> {
        String res = eval.call();
        return res;
      });
    }
  }
}
