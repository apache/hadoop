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
package org.apache.hadoop.ipc;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.RetryCache.CacheEntryWithPayload;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link RetryCache}
 */
public class TestRetryCache {
  private static final byte[] CLIENT_ID = ClientId.getClientId();
  private static int callId = 100;
  private static final Random r = new Random();
  private static final TestServer testServer = new TestServer();

  @Before
  public void setup() {
    testServer.resetCounters();
  }

  static class TestServer {
    AtomicInteger retryCount = new AtomicInteger();
    AtomicInteger operationCount = new AtomicInteger();
    private RetryCache retryCache = new RetryCache("TestRetryCache", 1,
        100 * 1000 * 1000 * 1000L);

    /**
     * A server method implemented using {@link RetryCache}.
     * 
     * @param input is returned back in echo, if {@code success} is true.
     * @param failureOuput returned on failure, if {@code success} is false.
     * @param methodTime time taken by the operation. By passing smaller/larger
     *          value one can simulate an operation that takes short/long time.
     * @param success whether this operation completes successfully or not
     * @return return the input parameter {@code input}, if {@code success} is
     *         true, else return {@code failureOutput}.
     */
    int echo(int input, int failureOutput, long methodTime, boolean success)
        throws InterruptedException {
      CacheEntryWithPayload entry = RetryCache.waitForCompletion(retryCache,
          null);
      if (entry != null && entry.isSuccess()) {
        System.out.println("retryCount incremented " + retryCount.get());
        retryCount.incrementAndGet();
        return (Integer) entry.getPayload();
      }
      try {
        operationCount.incrementAndGet();
        if (methodTime > 0) {
          Thread.sleep(methodTime);
        }
      } finally {
        RetryCache.setState(entry, success, input);
      }
      return success ? input : failureOutput;
    }

    void resetCounters() {
      retryCount.set(0);
      operationCount.set(0);
    }
  }

  public static Server.Call newCall() {
    return new Server.Call(++callId, 1, null, null,
        RpcKind.RPC_PROTOCOL_BUFFER, CLIENT_ID);
  }

  /**
   * This simlulates a long server retried operations. Multiple threads start an
   * operation that takes long time and finally succeeds. The retries in this
   * case end up waiting for the current operation to complete. All the retries
   * then complete based on the entry in the retry cache.
   */
  @Test
  public void testLongOperationsSuccessful() throws Exception {
    // Test long successful operations
    // There is no entry in cache expected when the first operation starts
    testOperations(r.nextInt(), 100, 20, true, false, newCall());
  }

  /**
   * This simlulates a long server operation. Multiple threads start an
   * operation that takes long time and finally fails. The retries in this case
   * end up waiting for the current operation to complete. All the retries end
   * up performing the operation again.
   */
  @Test
  public void testLongOperationsFailure() throws Exception {
    // Test long failed operations
    // There is no entry in cache expected when the first operation starts
    testOperations(r.nextInt(), 100, 20, false, false, newCall());
  }

  /**
   * This simlulates a short server operation. Multiple threads start an
   * operation that takes very short time and finally succeeds. The retries in
   * this case do not wait long for the current operation to complete. All the
   * retries then complete based on the entry in the retry cache.
   */
  @Test
  public void testShortOperationsSuccess() throws Exception {
    // Test long failed operations
    // There is no entry in cache expected when the first operation starts
    testOperations(r.nextInt(), 25, 0, false, false, newCall());
  }

  /**
   * This simlulates a short server operation. Multiple threads start an
   * operation that takes short time and finally fails. The retries in this case
   * do not wait for the current operation to complete. All the retries end up
   * performing the operation again.
   */
  @Test
  public void testShortOperationsFailure() throws Exception {
    // Test long failed operations
    // There is no entry in cache expected when the first operation starts
    testOperations(r.nextInt(), 25, 0, false, false, newCall());
  }

  @Test
  public void testRetryAfterSuccess() throws Exception {
    // Previous operation successfully completed
    Server.Call call = newCall();
    int input = r.nextInt();
    Server.getCurCall().set(call);
    testServer.echo(input, input + 1, 5, true);
    testOperations(input, 25, 0, true, true, call);
  }

  @Test
  public void testRetryAfterFailure() throws Exception {
    // Previous operation failed
    Server.Call call = newCall();
    int input = r.nextInt();
    Server.getCurCall().set(call);
    testServer.echo(input, input + 1, 5, false);
    testOperations(input, 25, 0, false, true, call);
  }

  public void testOperations(final int input, final int numberOfThreads,
      final int pause, final boolean success, final boolean attemptedBefore,
      final Server.Call call) throws InterruptedException, ExecutionException {
    final int failureOutput = input + 1;
    ExecutorService executorService = Executors
        .newFixedThreadPool(numberOfThreads);
    List<Future<Integer>> list = new ArrayList<Future<Integer>>();
    for (int i = 0; i < numberOfThreads; i++) {
      Callable<Integer> worker = new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          Server.getCurCall().set(call);
          Assert.assertEquals(Server.getCurCall().get(), call);
          int randomPause = pause == 0 ? pause : r.nextInt(pause);
          return testServer.echo(input, failureOutput, randomPause, success);
        }
      };
      Future<Integer> submit = executorService.submit(worker);
      list.add(submit);
    }

    Assert.assertEquals(numberOfThreads, list.size());
    for (Future<Integer> future : list) {
      if (success) {
        Assert.assertEquals(input, future.get().intValue());
      } else {
        Assert.assertEquals(failureOutput, future.get().intValue());
      }
    }

    if (success) {
      // If the operation was successful, all the subsequent operations
      // by other threads should be retries. Operation count should be 1.
      int retries = numberOfThreads + (attemptedBefore ? 0 : -1);
      Assert.assertEquals(1, testServer.operationCount.get());
      Assert.assertEquals(retries, testServer.retryCount.get());
    } else {
      // If the operation failed, all the subsequent operations
      // should execute once more, hence the retry count should be 0 and
      // operation count should be the number of tries
      int opCount = numberOfThreads + (attemptedBefore ? 1 : 0);
      Assert.assertEquals(opCount, testServer.operationCount.get());
      Assert.assertEquals(0, testServer.retryCount.get());
    }
  }
}
