/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs.services;

import java.lang.Thread.State;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import net.jodah.concurrentunit.Waiter;
import org.junit.Test;

import static java.lang.Thread.sleep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test class for AbfsByteBufferPool.
 */
public class TestAbfsByteBufferPool {

  private static final int TWO_MB = 2 * 1024 * 1024;

  private static final int MINUS_HUNDRED = -100;
  private static final int HUNDRED_AND_ONE = 101;

  private static final int MEM_USAGE_PC_20 = 20;
  private static final int MEM_USAGE_PC_25 = 25;
  private static final int MEM_USAGE_PC_30 = 30;
  private static final int MEM_USAGE_PC_90 = 90;

  private static final int SYNC_TEST_THREAD_CATEGORY_COUNT = 4;
  private static final int SYNC_TEST_THREAD_COUNT_PER_CATEGORY = 4;
  private static final int SYNC_TEST_VERIFICATION_PER_CATEGORY = 25;
  private static final int SYNC_TEST_THREAD_POOL_SIZE = 50;
  private static final int SYNC_TEST_MAX_FREE_BUFFER_COUNT = 4;
  private static final long SYNC_TEST_WAITER_WAIT_TIME_6000_MS = 6000;
  private static final long SYNC_TEST_VERIFIER_SLEEP_TIME_50_MS = 50;
  private static final long SYNC_TEST_NON_VERIFIER_SLEEP_TIME_100_MS = 100;

  private static final int BUFFER_SIZE_2 = 2;

  private static final int QUEUE_CAPACITY_2 = 2;
  private static final int QUEUE_CAPACITY_3 = 3;
  private static final int QUEUE_CAPACITY_5 = 5;

  private static final int MAX_FREE_BUFFERS_4 = 4;

  private static final long SLEEP_TIME_5000_MS = 5000;

  @Test
  public void testWithInvalidMaxWriteMemUsagePercentage() throws Exception {
    List<Integer> invalidMaxWriteMemUsagePercentageList = Arrays
        .asList(MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE - 1,
            MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE + 1, MINUS_HUNDRED,
            HUNDRED_AND_ONE);
    for (int val : invalidMaxWriteMemUsagePercentageList) {
      intercept(IllegalArgumentException.class, String
              .format("maxWriteMemUsagePercentage should be in range (%s - %s)",
                  MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE,
                  MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE),
          () -> new AbfsByteBufferPool(TWO_MB, QUEUE_CAPACITY_2, val));
    }
  }

  @Test
  public void testWithInvalidMaxFreeBuffers() throws Exception {
    List<Integer> invalidMaxFreeBuffers = Arrays.asList(0, -1);
    for (int val : invalidMaxFreeBuffers) {
      intercept(IllegalArgumentException.class, String
              .format("queueCapacity cannot be < 1",
                  MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE,
                  MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE),
          () -> new AbfsByteBufferPool(TWO_MB, val, MEM_USAGE_PC_20));
    }
  }

  @Test(expected = NullPointerException.class)
  public void testReleaseNull() {
    AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB, QUEUE_CAPACITY_5,
        MEM_USAGE_PC_30);
    pool.release(null);
  }

  @Test
  public void testReleaseMoreThanPoolCapacity() {
    AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB, MAX_FREE_BUFFERS_4,
        MEM_USAGE_PC_25);
    int expectedPoolCapacity = MAX_FREE_BUFFERS_4;
    for (int i = 0; i < expectedPoolCapacity * 2; i++) {
      pool.release(new byte[TWO_MB]);
      assertThat(pool.getFreeBuffers()).describedAs(
          "Pool size should never exceed the expected capacity irrespective "
              + "of the number of objects released to the pool")
          .hasSizeLessThanOrEqualTo(expectedPoolCapacity);
    }
  }

  @Test
  public void testReleaseWithSameBufferSize() {
    AbfsByteBufferPool pool = new AbfsByteBufferPool(BUFFER_SIZE_2,
        QUEUE_CAPACITY_3, MEM_USAGE_PC_25);
    pool.release(new byte[BUFFER_SIZE_2]);
  }

  @Test
  public void testReleaseWithDifferentBufferSize() throws Exception {
    AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB, QUEUE_CAPACITY_3,
        MEM_USAGE_PC_25);
    for (int i = 1; i < 2; i++) {
      int finalI = i;
      intercept(IllegalArgumentException.class,
          String.format("Buffer size has to be %s", TWO_MB),
          () -> pool.release(new byte[TWO_MB + finalI]));
      intercept(IllegalArgumentException.class,
          String.format("Buffer size has to be %s", TWO_MB),
          () -> pool.release(new byte[TWO_MB - finalI]));
    }
  }

  @Test
  public void testGet() throws Exception {
    int expectedMaxBuffersInUse =
        MAX_FREE_BUFFERS_4 + Runtime.getRuntime().availableProcessors();
    AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB, MAX_FREE_BUFFERS_4,
        MEM_USAGE_PC_90);

    //  Getting the maximum number of byte arrays from the pool
    byte[] byteBuffer = null;
    for (int i = 0; i < expectedMaxBuffersInUse; i++) {
      byteBuffer = pool.get();
      assertThat(byteBuffer.length).describedAs("Pool has to return an object "
          + "immediately, until maximum buffers are in use.").isEqualTo(TWO_MB);
    }

    //  Already maximum number of buffers are retrieved from the pool so the
    //  next get call is going to be blocked
    Thread getThread = new Thread(() -> pool.get());
    getThread.start();
    sleep(SLEEP_TIME_5000_MS);
    assertThat(getThread.getState()).describedAs("When maximum number of "
        + "buffers are in use and no free buffers available in the pool the "
        + "get call is blocked until an object is released to the pool.")
        .isEqualTo(State.WAITING);
    getThread.interrupt();

    //  Releasing one byte array back to the pool post which the get call we
    //  are making should not be blocking
    pool.release(byteBuffer);
    byteBuffer = null;
    byteBuffer = pool.get();
    assertThat(byteBuffer.length).describedAs("Pool has to return an object "
        + "immediately, if there are free buffers available in the pool.")
        .isEqualTo(TWO_MB);

    //  Again trying to get one byte buffer from the pool which will be
    //  blocked as the pool has already dished out the maximum number of byte
    //  arrays. Then we release another byte array and the blocked get call
    //  gets unblocked.
    Callable<byte[]> callable = () -> pool.get();
    FutureTask futureTask = new FutureTask(callable);
    getThread = new Thread(futureTask);
    getThread.start();
    pool.release(new byte[TWO_MB]);
    byteBuffer = (byte[]) futureTask.get();
    assertThat(byteBuffer.length).describedAs("The blocked get call unblocks "
        + "when an object is released back to the pool.").isEqualTo(TWO_MB);
  }

  @Test
  public void testSynchronisation() throws Throwable {
    final int expectedMaxBuffersInUse =
        SYNC_TEST_MAX_FREE_BUFFER_COUNT + Runtime.getRuntime()
            .availableProcessors();
    final AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB,
        SYNC_TEST_MAX_FREE_BUFFER_COUNT, MEM_USAGE_PC_90);

    final int totalThreadCount =
        SYNC_TEST_THREAD_CATEGORY_COUNT * SYNC_TEST_THREAD_COUNT_PER_CATEGORY;

    final LinkedBlockingQueue<byte[]> objsQueue = new LinkedBlockingQueue();
    final Waiter waiter = new Waiter();

    //  Creating 3 types of runnables
    //  getter: would make get calls
    //  releaser: would make release calls
    //  verifier: will be verifying the conditions
    final Runnable getter = getNewGetterRunnable(pool, objsQueue, waiter,
        expectedMaxBuffersInUse, SYNC_TEST_MAX_FREE_BUFFER_COUNT);
    final Runnable releaser = getNewReleaserRunnable(pool, objsQueue, waiter,
        expectedMaxBuffersInUse, SYNC_TEST_MAX_FREE_BUFFER_COUNT);
    final Runnable verifier = getNewVerifierRunnable(pool,
        expectedMaxBuffersInUse, SYNC_TEST_MAX_FREE_BUFFER_COUNT, waiter);

    final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
        .newFixedThreadPool(SYNC_TEST_THREAD_POOL_SIZE);
    for (int i = 0; i < SYNC_TEST_THREAD_COUNT_PER_CATEGORY; i++) {
      executor.submit(new Thread(getter));
      executor.submit(new Thread(releaser));
      executor.submit(new Thread(verifier));
      executor.submit(new Thread(verifier));
    }
    waiter.await(SYNC_TEST_WAITER_WAIT_TIME_6000_MS, totalThreadCount);
    executor.shutdown();
  }

  private Runnable getNewGetterRunnable(final AbfsByteBufferPool pool,
      final LinkedBlockingQueue<byte[]> objsQueue, final Waiter waiter,
      final int expectedMaxBuffersInUse, final int maxFreeBuffers) {
    Runnable runnable = () -> {
      for (int i = 0; i < SYNC_TEST_VERIFICATION_PER_CATEGORY; i++) {
        verify(pool, waiter, maxFreeBuffers, expectedMaxBuffersInUse);
        objsQueue.offer(pool.get());
        verify(pool, waiter, maxFreeBuffers, expectedMaxBuffersInUse);
        try {
          sleep(SYNC_TEST_NON_VERIFIER_SLEEP_TIME_100_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      waiter.resume();
    };
    return runnable;
  }

  private Runnable getNewReleaserRunnable(final AbfsByteBufferPool pool,
      final LinkedBlockingQueue<byte[]> objsQueue, final Waiter waiter,
      final int expectedMaxBuffersInUse, final int maxFreeBuffers) {
    Runnable runnable = () -> {
      for (int i = 0; i < SYNC_TEST_VERIFICATION_PER_CATEGORY; i++) {
        try {
          verify(pool, waiter, maxFreeBuffers, expectedMaxBuffersInUse);
          pool.release(objsQueue.take());
          verify(pool, waiter, maxFreeBuffers, expectedMaxBuffersInUse);
          sleep(SYNC_TEST_NON_VERIFIER_SLEEP_TIME_100_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      waiter.resume();
    };
    return runnable;
  }

  private Runnable getNewVerifierRunnable(final AbfsByteBufferPool pool,
      final int expectedMaxBuffersInUse, final int maxFreeBuffers,
      final Waiter waiter) {
    Runnable runnable = () -> {
      for (int i = 0; i < SYNC_TEST_VERIFICATION_PER_CATEGORY; i++) {
        verify(pool, waiter, maxFreeBuffers, expectedMaxBuffersInUse);
        try {
          sleep(SYNC_TEST_VERIFIER_SLEEP_TIME_50_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      waiter.resume();
    };
    return runnable;
  }

  private void verify(final AbfsByteBufferPool pool, final Waiter waiter,
      final int maxFreeBuffers, final int expectedMaxBuffersInUse) {
    waiter.assertThat(pool.getFreeBuffers(), is(notNullValue()));
    waiter.assertThat(pool.getFreeBuffers().size(),
        is(lessThanOrEqualTo(maxFreeBuffers)));
    waiter.assertThat(pool.getBuffersInUse(),
        is(lessThanOrEqualTo(expectedMaxBuffersInUse)));
    waiter.assertThat(pool.getBuffersInUse(), is(greaterThanOrEqualTo(0)));
  }

}
