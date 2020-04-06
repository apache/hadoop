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

import org.junit.Test;
import net.jodah.concurrentunit.Waiter;

import static java.lang.Thread.sleep;

import static org.assertj.core.api.Assertions.assertThat;;
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

  @Test
  public void testWithInvalidMaxWriteMemUsagePercentage() throws Exception {
    List<Integer> invalidMaxWriteMemUsagePercentageList = Arrays
        .asList(MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE - 1,
            MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE + 1, -100, 101);
    for (int val : invalidMaxWriteMemUsagePercentageList) {
      intercept(IllegalArgumentException.class, String
              .format("maxWriteMemUsagePercentage should be in range (%s - %s)",
                  MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE,
                  MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE),
          () -> new AbfsByteBufferPool(TWO_MB, 2, val));
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
          () -> new AbfsByteBufferPool(TWO_MB, val, 20));
    }
  }

  @Test(expected = NullPointerException.class)
  public void testReleaseNull() {
    AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB, 5, 30);
    pool.release(null);
  }

  @Test
  public void testReleaseMoreThanPoolCapacity() {
    int maxFreeBuffers = 4;
    AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB, maxFreeBuffers,
        25);
    int expectedPoolCapacity = maxFreeBuffers;
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
    int bufferSize = 2;
    AbfsByteBufferPool pool = new AbfsByteBufferPool(2, 3, 25);
    pool.release(new byte[bufferSize]);
  }

  @Test
  public void testReleaseWithDifferentBufferSize() throws Exception {
    String errorString = "Buffer size has to be %s";
    AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB, 3, 25);
    for (int i = 1; i < 2; i++) {
      int finalI = i;
      intercept(IllegalArgumentException.class,
          String.format(errorString, TWO_MB),
          () -> pool.release(new byte[TWO_MB + finalI]));
      intercept(IllegalArgumentException.class,
          String.format(errorString, TWO_MB),
          () -> pool.release(new byte[TWO_MB - finalI]));
    }
  }

  @Test
  public void testGet() throws Exception {
    int maxFreeBuffers = 4;
    int expectedMaxBuffersInUse =
        maxFreeBuffers + Runtime.getRuntime().availableProcessors();
    AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB, maxFreeBuffers,
        90);

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
    sleep(5000);
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
    final int maxFreeBuffers = 4;
    final int expectedMaxBuffersInUse =
        maxFreeBuffers + Runtime.getRuntime().availableProcessors();
    final AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB,
        maxFreeBuffers, 90);

    final int numCategoryOfThreads = 4;
    final int numThreadsForEachCategory = 4;
    final int totalThreadCount =
        numCategoryOfThreads * numThreadsForEachCategory;

    final LinkedBlockingQueue<byte[]> objsQueue = new LinkedBlockingQueue();
    final Waiter waiter = new Waiter();

    //  Creating 3 types of runnables
    //  getter: would make get calls
    //  releaser: would make release calls
    //  verifier: will be verifying the conditions
    final Runnable getter = getNewGetterRunnable(pool, objsQueue, waiter,
        expectedMaxBuffersInUse, maxFreeBuffers);
    final Runnable releaser = getNewReleaserRunnable(pool, objsQueue, waiter,
        expectedMaxBuffersInUse, maxFreeBuffers);
    final Runnable verifier = getNewVerifierRunnable(pool,
        expectedMaxBuffersInUse, maxFreeBuffers, waiter);

    final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
        .newFixedThreadPool(50);
    for (int i = 0; i < numThreadsForEachCategory; i++) {
      executor.submit(new Thread(getter));
      executor.submit(new Thread(releaser));
      executor.submit(new Thread(verifier));
      executor.submit(new Thread(verifier));
    }
    waiter.await(60000, totalThreadCount);
    executor.shutdown();
  }

  private Runnable getNewGetterRunnable(final AbfsByteBufferPool pool,
      final LinkedBlockingQueue<byte[]> objsQueue, final Waiter waiter,
      final int expectedMaxBuffersInUse, final int maxFreeBuffers) {
    Runnable runnable = () -> {
      for (int i = 0; i < 25; i++) {
        verify(pool, waiter, maxFreeBuffers, expectedMaxBuffersInUse);
        objsQueue.offer(pool.get());
        verify(pool, waiter, maxFreeBuffers, expectedMaxBuffersInUse);
        try {
          sleep(100);
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
      for (int i = 0; i < 25; i++) {
        try {
          verify(pool, waiter, maxFreeBuffers, expectedMaxBuffersInUse);
          pool.release(objsQueue.take());
          verify(pool, waiter, maxFreeBuffers, expectedMaxBuffersInUse);
          sleep(100);
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
      for (int i = 0; i < 25; i++) {
        verify(pool, waiter, maxFreeBuffers, expectedMaxBuffersInUse);
        try {
          sleep(50);
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
