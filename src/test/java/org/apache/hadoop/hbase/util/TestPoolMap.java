/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.hbase.util.PoolMap.PoolType;

public class TestPoolMap {
  public abstract static class TestPoolType extends TestCase {
    protected PoolMap<String, String> poolMap;
    protected Random random = new Random();

    protected static final int POOL_SIZE = 3;

    @Override
    protected void setUp() throws Exception {
      this.poolMap = new PoolMap<String, String>(getPoolType(), POOL_SIZE);
    }

    protected abstract PoolType getPoolType();

    @Override
    protected void tearDown() throws Exception {
      this.poolMap.clear();
    }

    protected void runThread(final String randomKey, final String randomValue,
        final String expectedValue) throws InterruptedException {
      final AtomicBoolean matchFound = new AtomicBoolean(false);
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          poolMap.put(randomKey, randomValue);
          String actualValue = poolMap.get(randomKey);
          matchFound.set(expectedValue == null ? actualValue == null
              : expectedValue.equals(actualValue));
        }
      });
      thread.start();
      thread.join();
      assertTrue(matchFound.get());
    }
  }

  public static class TestRoundRobinPoolType extends TestPoolType {
    @Override
    protected PoolType getPoolType() {
      return PoolType.RoundRobin;
    }

    public void testSingleThreadedClient() throws InterruptedException,
        ExecutionException {
      String randomKey = String.valueOf(random.nextInt());
      String randomValue = String.valueOf(random.nextInt());
      // As long as the pool is not full, we'll get null back.
      // This forces the user to create new values that can be used to populate
      // the pool.
      runThread(randomKey, randomValue, null);
      assertEquals(1, poolMap.size(randomKey));
    }

    public void testMultiThreadedClients() throws InterruptedException,
        ExecutionException {
      for (int i = 0; i < POOL_SIZE; i++) {
        String randomKey = String.valueOf(random.nextInt());
        String randomValue = String.valueOf(random.nextInt());
        // As long as the pool is not full, we'll get null back
        runThread(randomKey, randomValue, null);
        // As long as we use distinct keys, each pool will have one value
        assertEquals(1, poolMap.size(randomKey));
      }
      poolMap.clear();
      String randomKey = String.valueOf(random.nextInt());
      for (int i = 0; i < POOL_SIZE - 1; i++) {
        String randomValue = String.valueOf(random.nextInt());
        // As long as the pool is not full, we'll get null back
        runThread(randomKey, randomValue, null);
        // since we use the same key, the pool size should grow
        assertEquals(i + 1, poolMap.size(randomKey));
      }
      // at the end of the day, there should be as many values as we put
      assertEquals(POOL_SIZE - 1, poolMap.size(randomKey));
    }

    public void testPoolCap() throws InterruptedException, ExecutionException {
      String randomKey = String.valueOf(random.nextInt());
      List<String> randomValues = new ArrayList<String>();
      for (int i = 0; i < POOL_SIZE * 2; i++) {
        String randomValue = String.valueOf(random.nextInt());
        randomValues.add(randomValue);
        if (i < POOL_SIZE - 1) {
          // As long as the pool is not full, we'll get null back
          runThread(randomKey, randomValue, null);
        } else {
          // when the pool becomes full, we expect the value we get back to be
          // what we put earlier, in round-robin order
          runThread(randomKey, randomValue,
              randomValues.get((i - POOL_SIZE + 1) % POOL_SIZE));
        }
      }
      assertEquals(POOL_SIZE, poolMap.size(randomKey));
    }

  }

  public static class TestThreadLocalPoolType extends TestPoolType {
    @Override
    protected PoolType getPoolType() {
      return PoolType.ThreadLocal;
    }

    public void testSingleThreadedClient() throws InterruptedException,
        ExecutionException {
      String randomKey = String.valueOf(random.nextInt());
      String randomValue = String.valueOf(random.nextInt());
      // As long as the pool is not full, we should get back what we put
      runThread(randomKey, randomValue, randomValue);
      assertEquals(1, poolMap.size(randomKey));
    }

    public void testMultiThreadedClients() throws InterruptedException,
        ExecutionException {
      // As long as the pool is not full, we should get back what we put
      for (int i = 0; i < POOL_SIZE; i++) {
        String randomKey = String.valueOf(random.nextInt());
        String randomValue = String.valueOf(random.nextInt());
        runThread(randomKey, randomValue, randomValue);
        assertEquals(1, poolMap.size(randomKey));
      }
      String randomKey = String.valueOf(random.nextInt());
      for (int i = 0; i < POOL_SIZE; i++) {
        String randomValue = String.valueOf(random.nextInt());
        runThread(randomKey, randomValue, randomValue);
        assertEquals(i + 1, poolMap.size(randomKey));
      }
    }

    public void testPoolCap() throws InterruptedException, ExecutionException {
      String randomKey = String.valueOf(random.nextInt());
      for (int i = 0; i < POOL_SIZE * 2; i++) {
        String randomValue = String.valueOf(random.nextInt());
        if (i < POOL_SIZE) {
          runThread(randomKey, randomValue, randomValue);
        } else {
          // When the pool fills up, we should not be able to put any new values
          runThread(randomKey, randomValue, null);
        }
      }
    }

  }

  public static class TestReusablePoolType extends TestPoolType {
    @Override
    protected PoolType getPoolType() {
      return PoolType.Reusable;
    }

    public void testSingleThreadedClient() throws InterruptedException,
        ExecutionException {
      String randomKey = String.valueOf(random.nextInt());
      String randomValue = String.valueOf(random.nextInt());
      // As long as we poll values we put, the pool size should remain zero
      runThread(randomKey, randomValue, randomValue);
      assertEquals(0, poolMap.size(randomKey));
    }

    public void testMultiThreadedClients() throws InterruptedException,
        ExecutionException {
      // As long as we poll values we put, the pool size should remain zero
      for (int i = 0; i < POOL_SIZE; i++) {
        String randomKey = String.valueOf(random.nextInt());
        String randomValue = String.valueOf(random.nextInt());
        runThread(randomKey, randomValue, randomValue);
        assertEquals(0, poolMap.size(randomKey));
      }
      poolMap.clear();
      String randomKey = String.valueOf(random.nextInt());
      for (int i = 0; i < POOL_SIZE - 1; i++) {
        String randomValue = String.valueOf(random.nextInt());
        runThread(randomKey, randomValue, randomValue);
        assertEquals(0, poolMap.size(randomKey));
      }
      assertEquals(0, poolMap.size(randomKey));
    }

    public void testPoolCap() throws InterruptedException, ExecutionException {
      // As long as we poll values we put, the pool size should remain zero
      String randomKey = String.valueOf(random.nextInt());
      List<String> randomValues = new ArrayList<String>();
      for (int i = 0; i < POOL_SIZE * 2; i++) {
        String randomValue = String.valueOf(random.nextInt());
        randomValues.add(randomValue);
        runThread(randomKey, randomValue, randomValue);
      }
      assertEquals(0, poolMap.size(randomKey));
    }

  }

  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTestSuite(TestRoundRobinPoolType.class);
    suite.addTestSuite(TestThreadLocalPoolType.class);
    suite.addTestSuite(TestReusablePoolType.class);
    return suite;
  }

}
