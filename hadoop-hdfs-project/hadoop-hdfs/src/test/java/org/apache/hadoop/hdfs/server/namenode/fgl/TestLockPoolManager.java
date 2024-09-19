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
package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.apache.hadoop.test.LambdaTestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hdfs.server.namenode.fgl.LockPoolManager.LockMode;
import org.apache.hadoop.util.Time;

public class TestLockPoolManager {

  private LockPoolManager<String> poolManager;

  @After
  public void cleanup() {
    poolManager.close();
  }

  @Test
  public void testAcquireReadLock() throws Exception {
    poolManager = new LockPoolManager<>(100, -1, false);

    String key1 = "mockKey1";
    String key2 = "mockKey2";

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> poolManager.acquireLock(null, LockMode.READ));

    AutoCloseableLockInPool<String> lock1 = poolManager.acquireLock(key1, LockMode.READ);
    Assert.assertEquals(1, poolManager.getPoolSize());

    AutoCloseableLockInPool<String> lock2 = poolManager.acquireLock(key1, LockMode.READ);
    Assert.assertEquals(1, poolManager.getPoolSize());

    AutoCloseableLockInPool<String> lock3 = poolManager.acquireLock(key2, LockMode.READ);
    Assert.assertEquals(2, poolManager.getPoolSize());

    LambdaTestUtils.intercept(IllegalStateException.class,
        () -> poolManager.acquireLock(key1, LockMode.WRITE));

    lock1.close();
    Assert.assertEquals(2, poolManager.getPoolSize());
    lock2.close();
    Assert.assertEquals(1, poolManager.getPoolSize());
    lock3.close();
    Assert.assertEquals(0, poolManager.getPoolSize());

    LambdaTestUtils.intercept(IllegalStateException.class, lock3::close);

    AutoCloseableLockInPool<String> ignore = poolManager.acquireLock(key1, LockMode.READ);
    Assert.assertEquals(1, poolManager.getPoolSize());
  }

  @Test
  public void testAcquireWriteLock() throws Exception {
    poolManager = new LockPoolManager<>(100, -1, false);

    String key1 = "mockKey1";
    String key2 = "mockKey2";

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> poolManager.acquireLock(null, LockMode.WRITE));

    AutoCloseableLockInPool<String> lock1 = poolManager.acquireLock(key1, LockMode.WRITE);
    Assert.assertEquals(1, poolManager.getPoolSize());

    AutoCloseableLockInPool<String> lock2 = poolManager.acquireLock(key1, LockMode.READ);
    Assert.assertEquals(1, poolManager.getPoolSize());

    AutoCloseableLockInPool<String> lock3 = poolManager.acquireLock(key2, LockMode.WRITE);
    Assert.assertEquals(2, poolManager.getPoolSize());

    lock1.close();
    Assert.assertEquals(2, poolManager.getPoolSize());
    lock2.close();
    Assert.assertEquals(1, poolManager.getPoolSize());
    lock3.close();
    Assert.assertEquals(0, poolManager.getPoolSize());

    LambdaTestUtils.intercept(IllegalStateException.class, lock3::close);
  }

  @Test
  public void testChangeLock() throws Exception {
    poolManager = new LockPoolManager<>(100, -1, false);

    String key1 = "mockKey1";

    AutoCloseableLockInPool<String> lock1 = poolManager.acquireLock(key1, LockMode.READ);
    Assert.assertEquals(1, poolManager.getPoolSize());
    Assert.assertTrue(lock1.hasReadLock());

    lock1.changeToWriteMode();
    Assert.assertEquals(1, poolManager.getPoolSize());
    Assert.assertTrue(lock1.hasWriteLock());
    Assert.assertEquals(0, lock1.getReadHoldCount());

    lock1.changeToReadMode();
    Assert.assertEquals(1, poolManager.getPoolSize());
    Assert.assertFalse(lock1.hasWriteLock());
    Assert.assertTrue(lock1.hasReadLock());
    Assert.assertEquals(1, lock1.getReadHoldCount());

    lock1.close();

    LambdaTestUtils.intercept(IllegalStateException.class, lock1::changeToReadMode);
    LambdaTestUtils.intercept(IllegalStateException.class, lock1::changeToWriteMode);
  }

  @Test(timeout = 60000)
  public void testPoolManagerWithMultipleThread() throws InterruptedException {
    poolManager = new LockPoolManager<>(100, -1, false);

    ExecutorService executors = Executors.newFixedThreadPool(10000);
    List<Worker> workerList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      workerList.add(new Worker(poolManager));
    }

    executors.invokeAll(workerList);

    Assert.assertEquals(0, poolManager.getPoolSize());
  }

  @Test
  public void testPoolManagerPromotion() {
    poolManager = new LockPoolManager<>(100, 10, false);

    int N = 20;
    String[] keys = new String[N];
    for (int i = 0; i < N; i++) {
      keys[i] = "key" + i;
      for (int j = N; j >= i; j--) {
        poolManager.acquireLock(keys[i], LockMode.READ);
        poolManager.releaseLockResource(keys[i]);
      }
    }
    poolManager.promoteLocks();

    List<String> cachedKeys = poolManager.getCachedKeys();
    Assert.assertEquals(10, cachedKeys.size());
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(cachedKeys.contains(keys[i]));
    }
  }

  public static void main(String[] args) throws Exception {
    int MAP_SIZE = 1000;
    int CACHE_SIZE = 200;
    int BINS = 10000;
    int WORKERS = 20;

    LockPoolManager<Integer> poolManager = new LockPoolManager<>(MAP_SIZE, CACHE_SIZE, false);

    ExecutorService executors = Executors.newFixedThreadPool(WORKERS);
    List<BenchWorker> workerList = new ArrayList<>();
    for (int i = 0; i < WORKERS; i++) {
      workerList.add(new BenchWorker(poolManager, BINS));
    }

    for (BenchWorker worker : workerList) {
      executors.submit(worker);
    }

    while (true) {
      long last = Time.monotonicNow();
      long snapshotLast = 0;
      for (BenchWorker worker : workerList) {
        snapshotLast += worker.counter;
      }
      Thread.sleep(10000);
      long now = Time.monotonicNow();
      long snapshotNow = 0;
      for (BenchWorker worker : workerList) {
        snapshotNow += worker.counter;
      }
      long elapsed = now - last;
      long rate = (snapshotNow - snapshotLast) / elapsed;
      System.out.printf("d=%d ms,r=%d ops/s%n", elapsed, rate);
    }
  }

  static class BenchWorker implements Callable<Void> {
    private final LockPoolManager<Integer> poolManager;
    private final int bins;
    private long counter;
    private final static int BIASED_BINS1 = 10 << 4;
    private final static int BIASED_BINS2 = 100 << 4;

    BenchWorker(LockPoolManager<Integer> poolManager, int bins) {
      this.poolManager = poolManager;
      this.bins = bins << 4;
      this.counter = 0;
    }

    @Override
    public Void call() throws Exception {
      while (true) {
        // 10% hits go for the first 10 elements, 15% for the first 100, 75% for the rest
        double bias = ThreadLocalRandom.current().nextDouble();
        int finalBins = bias < 0.1 ? BIASED_BINS1 : bias < 0.25 ? BIASED_BINS2 : bins;
        int roll = ThreadLocalRandom.current().nextInt(finalBins);
        int bin = roll >> 4;
        int check = roll & 0b1111;

        LockMode mode = check < 4 ? LockMode.WRITE : LockMode.READ;
        try (AutoCloseableLockInPool<Integer> lock = poolManager.acquireLock(bin, mode)) {
          if (check == 0) {
            lock.changeToReadMode();
          } else if (check == 15) {
            lock.changeToWriteMode();
          }
        }
        counter++;
      }
    }
  }

  static class Worker implements Callable<Void> {
    final LockPoolManager<String> poolManager;

    Worker(LockPoolManager<String> poolManager) {
      this.poolManager = poolManager;
    }

    @Override
    public Void call() throws Exception {
      for (int i = 0; i < 1000; i++) {
        int index = ThreadLocalRandom.current().nextInt(10);
        LockMode mode = index <= 3 ? LockMode.WRITE : LockMode.READ;
        AutoCloseableLockInPool<String> lock = poolManager.acquireLock(String.valueOf(index), mode);
        if (index == 2) {
          lock.changeToReadMode();
        } else if (index == 5) {
          lock.changeToWriteMode();
        }
        lock.close();
      }
      return null;
    }
  }
}
