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

package org.apache.hadoop.fs.common;

import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestExecutorServiceFuturePool {
  @Test
  public void testRunnableSucceeds() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    try {
      ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService);
      final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
      Future<Void> future = futurePool.executeRunnable(() -> atomicBoolean.set(true));
      future.get(30, TimeUnit.SECONDS);
      assertTrue(atomicBoolean.get());
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testSupplierSucceeds() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    try {
      ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService);
      final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
      Future<Void> future = futurePool.executeFunction(() -> {
        atomicBoolean.set(true);
        return null;
      });
      future.get(30, TimeUnit.SECONDS);
      assertTrue(atomicBoolean.get());
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testRunnableFails() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    try {
      ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService);
      Future<Void> future = futurePool.executeRunnable(() -> {
        throw new IllegalStateException("deliberate");
      });
      assertThrows(ExecutionException.class, () -> future.get(30, TimeUnit.SECONDS));
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testSupplierFails() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    try {
      ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService);
      Future<Void> future = futurePool.executeFunction(() -> {
        throw new IllegalStateException("deliberate");
      });
      assertThrows(ExecutionException.class, () -> future.get(30, TimeUnit.SECONDS));
    } finally {
      executorService.shutdownNow();
    }
  }
}
