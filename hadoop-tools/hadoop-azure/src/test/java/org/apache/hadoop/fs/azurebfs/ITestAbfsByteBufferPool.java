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
              .format("maxFreeBuffers cannot be < 1",
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
    AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB,
        maxFreeBuffers, 25);
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
    AbfsByteBufferPool pool = new AbfsByteBufferPool(TWO_MB,
        maxFreeBuffers, 90);

    for (int i = 0; i < expectedMaxBuffersInUse; i++) {
      byte[] byteBuffer = pool.get();
      assertThat(byteBuffer.length).describedAs("Pool has to return an object "
          + "immediately, until maximum buffers are in use.").isEqualTo(TWO_MB);
    }

    pool.release(new byte[2 * 1024 * 1024]);
    byte[] byteBuffer = pool.get();
    assertThat(byteBuffer.length).describedAs("Pool has to return an object "
        + "immediately, if there are free buffers available in the pool.")
        .isEqualTo(TWO_MB);

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
    pool.release(new byte[TWO_MB]);
    byteBuffer = (byte[]) futureTask.get();
    assertThat(byteBuffer.length).describedAs("The blocked get call unblocks "
        + "when an object is released back to the pool.").isEqualTo(TWO_MB);
  }

  @Test
  public void testMaxBuffersInUse() {
    List<Object[]> testData = Arrays.asList(
        new Object[][] {{1, 1, 20}, {1, 100000, 90}, {1, 2, 30},
            {100, 100, 90}});
    for (int i = 0; i < testData.size(); i++) {
      int bufferSize = (int) testData.get(i)[0] * 1024 * 1024;
      int maxFreeBuffers = (int) testData.get(i)[1];
      int maxWriteMemUsagePercentage = (int) testData.get(i)[2];
      AbfsByteBufferPool pool = new AbfsByteBufferPool(bufferSize,
          maxFreeBuffers, maxWriteMemUsagePercentage);

      double maxMemoryAllowedForPoolMB =
          Runtime.getRuntime().maxMemory() * maxWriteMemUsagePercentage / 100;
      double bufferCountByMemory = maxMemoryAllowedForPoolMB / bufferSize;
      double bufferCountByMaxFreeBuffers =
          maxFreeBuffers + Runtime.getRuntime().availableProcessors();

      int expectedMaxBuffersInUse = (int) Math
          .ceil(Math.min(bufferCountByMemory, bufferCountByMaxFreeBuffers));
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
              + "calculated by maxFreeBuffers")
          .isLessThanOrEqualTo((int) Math.ceil(bufferCountByMemory));
    }
  }
}
