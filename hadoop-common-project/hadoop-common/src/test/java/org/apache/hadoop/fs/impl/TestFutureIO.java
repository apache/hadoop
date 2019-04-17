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

package org.apache.hadoop.fs.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.test.HadoopTestBase;
import org.apache.hadoop.util.LambdaUtils;

/**
 * Test behavior of {@link FutureIOSupport}, especially "what thread do things
 * happen in?".
 */
public class TestFutureIO extends HadoopTestBase {

  private ThreadLocal<AtomicInteger> local;

  @Before
  public void setup() throws Exception {
    local = ThreadLocal.withInitial(() -> new AtomicInteger(1));
  }

  /**
   * Simple eval is blocking and executes in the same thread.
   */
  @Test
  public void testEvalInCurrentThread() throws Throwable {
    CompletableFuture<Integer> result = new CompletableFuture<>();
    CompletableFuture<Integer> eval = LambdaUtils.eval(result,
        () -> {
          return getLocal().addAndGet(2);
        });
    assertEquals("Thread local value", 3, getLocalValue());
    assertEquals("Evaluated Value", 3, eval.get().intValue());
  }

  /**
   * A supply async call runs things in a shared thread pool.
   */
  @Test
  public void testEvalAsync() throws Throwable {
    final CompletableFuture<Integer> eval = CompletableFuture.supplyAsync(
        () -> getLocal().addAndGet(2));
    assertEquals("Thread local value", 1, getLocalValue());
    assertEquals("Evaluated Value", 3, eval.get().intValue());
  }


  protected AtomicInteger getLocal() {
    return local.get();
  }

  protected int getLocalValue() {
    return local.get().get();
  }
}
