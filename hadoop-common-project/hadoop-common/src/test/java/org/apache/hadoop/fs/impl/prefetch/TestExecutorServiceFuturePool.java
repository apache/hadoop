/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl.prefetch;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.junit.Assert.assertTrue;

public class TestExecutorServiceFuturePool extends AbstractHadoopTestBase {

  private ExecutorService executorService;

  @Before
  public void setUp() {
    executorService = Executors.newFixedThreadPool(3);
  }

  @After
  public void tearDown() {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testRunnableSucceeds() throws Exception {
    ExecutorServiceFuturePool futurePool =
        new ExecutorServiceFuturePool(executorService);
    final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
    Future<Void> future =
        futurePool.executeRunnable(() -> atomicBoolean.set(true));
    future.get(30, TimeUnit.SECONDS);
    assertTrue("atomicBoolean set to true?", atomicBoolean.get());
  }

  @Test
  public void testSupplierSucceeds() throws Exception {
    ExecutorServiceFuturePool futurePool =
        new ExecutorServiceFuturePool(executorService);
    final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
    Future<Void> future = futurePool.executeFunction(() -> {
      atomicBoolean.set(true);
      return null;
    });
    future.get(30, TimeUnit.SECONDS);
    assertTrue("atomicBoolean set to true?", atomicBoolean.get());
  }

  @Test
  public void testRunnableFails() throws Exception {
    ExecutorServiceFuturePool futurePool =
        new ExecutorServiceFuturePool(executorService);
    Future<Void> future = futurePool.executeRunnable(() -> {
      throw new IllegalStateException("deliberate");
    });
    interceptFuture(IllegalStateException.class, "deliberate", 30,
        TimeUnit.SECONDS, future);
  }

  @Test
  public void testSupplierFails() throws Exception {
    ExecutorServiceFuturePool futurePool =
        new ExecutorServiceFuturePool(executorService);
    Future<Void> future = futurePool.executeFunction(() -> {
      throw new IllegalStateException("deliberate");
    });
    interceptFuture(IllegalStateException.class, "deliberate", 30,
        TimeUnit.SECONDS, future);
  }
}
