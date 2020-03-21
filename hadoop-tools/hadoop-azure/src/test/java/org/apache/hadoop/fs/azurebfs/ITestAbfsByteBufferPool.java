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
package org.apache.hadoop.fs.azurebfs;

import java.lang.Thread.State;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.services.AbfsByteBufferPool;

import static org.assertj.core.api.Assertions.assertThat;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test class for AbfsByteBufferPool.
 */
public class ITestAbfsByteBufferPool {

  @Test
  public void testWithInvalidMaxWriteMemUsagePercentage() throws Exception {
    List<Integer> invalidMaxWriteMemUsagePercentageList = Arrays
        .asList(MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE - 1,
            MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE + 1, -100, 101);
    for (int val : invalidMaxWriteMemUsagePercentageList) {
      intercept(IllegalArgumentException.class, String
              .format("maxConcurrentThreadCount should be in range (%s - %s)",
                  MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE,
                  MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE),
          () -> new AbfsByteBufferPool(2, 2, val));
    }
  }

  @Test
  public void testWithInvalidMaxConcurrentThreadCount() throws Exception {
    List<Integer> invalidMaxConcurrentThreadCount = Arrays.asList(0, -1);
    for (int val : invalidMaxConcurrentThreadCount) {
      intercept(IllegalArgumentException.class, String
              .format("maxConcurrentThreadCount cannot be < 1",
                  MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE,
                  MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE),
          () -> new AbfsByteBufferPool(2, val, 20));
    }
  }

  @Test(expected = NullPointerException.class)
  public void testReleaseNull() {
    AbfsByteBufferPool pool = new AbfsByteBufferPool(2, 5, 30);
    pool.release(null);
  }

  @Test
  public void testReleaseMoreThanPoolCapacity() {
    int bufferSize = 2;
    int maxConcurrentThreadCount = 3;
    AbfsByteBufferPool pool = new AbfsByteBufferPool(bufferSize,
        maxConcurrentThreadCount, 25);
    int expectedPoolCapacity = maxConcurrentThreadCount + 1;
    for (int i = 0; i < expectedPoolCapacity * 2; i++) {
      pool.release(new byte[bufferSize]);
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
    int bufferSize = 2;
    String errorString = "Buffer size has to be %s";
    AbfsByteBufferPool pool = new AbfsByteBufferPool(bufferSize, 3, 25);
    for (int i = 1; i < 2; i++) {
      int finalI = i;
      intercept(IllegalArgumentException.class,
          String.format(errorString, bufferSize),
          () -> pool.release(new byte[bufferSize + finalI]));
      intercept(IllegalArgumentException.class,
          String.format(errorString, bufferSize),
          () -> pool.release(new byte[bufferSize - finalI]));
    }
  }

  @Test
  public void testGet() throws Exception {
    int bufferSize = 2;
    int maxConcurrentThreadCount = 3;
    int expectedMaxBuffersInUse =
        maxConcurrentThreadCount + Runtime.getRuntime().availableProcessors()
            + 1;
    AbfsByteBufferPool pool = new AbfsByteBufferPool(bufferSize,
        maxConcurrentThreadCount, 90);

    for (int i = 0; i < expectedMaxBuffersInUse; i++) {
      byte[] byteBuffer = pool.get();
      assertThat(byteBuffer.length).describedAs("Pool has to return an object "
          + "immediately, until maximum buffers are in use.")
          .isEqualTo(bufferSize);
    }

    byte[] byteBuffer = pool.get();
    pool.getFreeBuffers().add(new byte[bufferSize]);
    assertThat(byteBuffer.length).describedAs("Pool has to return an object "
        + "immediately, if thre is free buffers available in the pool.")
        .isEqualTo(bufferSize);

    Thread getThread = new Thread(() -> pool.get());
    getThread.start();
    Thread.sleep(5000);
    assertThat(getThread.getState()).describedAs("When maximum number of "
        + "buffers are in use and no free buffers available in the pool the "
        + "get call is blocked until an object is released to the pool.")
        .isEqualTo(State.WAITING);
    getThread.interrupt();

    Callable<byte[]> callable = () -> pool.get();
    FutureTask futureTask = new FutureTask(callable);
    getThread = new Thread(futureTask);
    getThread.start();
    pool.release(new byte[bufferSize]);
    byteBuffer = (byte[]) futureTask.get();
    assertThat(byteBuffer.length).describedAs("The blocked get call unblocks "
        + "when an object is released back to the pool.").isEqualTo(bufferSize);
  }

  @Test
  public void testMaxBuffersInUse() {
    List<Object[]> testData = Arrays.asList(
        new Object[][] {{1, 1, 20}, {1, 100000, 90}, {1, 2, 30},
            {100, 100, 90}});
    for (int i = 0; i < testData.size(); i++) {
      int bufferSize = (int) testData.get(i)[0];
      int maxConcurrentThreadCount = (int) testData.get(i)[1];
      int maxWriteMemUsagePercentage = (int) testData.get(i)[2];
      AbfsByteBufferPool pool = new AbfsByteBufferPool(bufferSize,
          maxConcurrentThreadCount, maxWriteMemUsagePercentage);

      double maxMemoryAllowedForPoolMB =
          Runtime.getRuntime().maxMemory() / (1024 * 1024)
              * maxWriteMemUsagePercentage / 100;
      double bufferCountByMemory = maxMemoryAllowedForPoolMB / bufferSize;
      double bufferCountByConcurrency =
          maxConcurrentThreadCount + Runtime.getRuntime().availableProcessors()
              + 1;
      int expectedMaxBuffersInUse = (int) Math
          .ceil(Math.min(bufferCountByMemory, bufferCountByConcurrency));
      if (expectedMaxBuffersInUse < 2) {
        expectedMaxBuffersInUse = 2;
      }

      assertThat(pool.getMaxBuffersInUse())
          .describedAs("Max buffers in use should be always greater than 1")
          .isGreaterThan(1)
          .describedAs("Max buffers in use should be equal to as expected")
          .isEqualTo(expectedMaxBuffersInUse).describedAs(
          "Max buffers in use should be <= number of "
              + "buffers calculated by memory percentage")
          .isLessThanOrEqualTo((int) Math.ceil(bufferCountByMemory))
          .describedAs("Max buffers in use should <= number of buffers "
              + "calculated by concurrency")
          .isLessThanOrEqualTo((int) Math.ceil(bufferCountByMemory));
    }
  }
}
