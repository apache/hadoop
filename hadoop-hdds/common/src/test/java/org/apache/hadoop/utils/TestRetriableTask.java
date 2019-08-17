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
package org.apache.hadoop.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipException;

/**
 * Tests for {@link RetriableTask}.
 */
public class TestRetriableTask {

  @Test
  public void returnsSuccessfulResult() throws Exception {
    String result = "bilbo";
    RetriableTask<String> task = new RetriableTask<>(
        RetryPolicies.RETRY_FOREVER, "test", () -> result);
    assertEquals(result, task.call());
  }

  @Test
  public void returnsSuccessfulResultAfterFailures() throws Exception {
    String result = "gandalf";
    AtomicInteger attempts = new AtomicInteger();
    RetriableTask<String> task = new RetriableTask<>(
        RetryPolicies.RETRY_FOREVER, "test",
        () -> {
          if (attempts.incrementAndGet() <= 2) {
            throw new Exception("testing");
          }
          return result;
        });
    assertEquals(result, task.call());
  }

  @Test
  public void respectsRetryPolicy() {
    int expectedAttempts = 3;
    AtomicInteger attempts = new AtomicInteger();
    RetryPolicy retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        expectedAttempts, 1, TimeUnit.MILLISECONDS);
    RetriableTask<String> task = new RetriableTask<>(retryPolicy, "thr", () -> {
      attempts.incrementAndGet();
      throw new ZipException("testing");
    });

    IOException e = assertThrows(IOException.class, task::call);
    assertEquals(ZipException.class, e.getCause().getClass());
    assertEquals(expectedAttempts, attempts.get());
  }

}
