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
package org.apache.hadoop.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * A test class for InstrumentedLock.
 */
public class TestInstrumentedLock {

  static final Logger LOG = LoggerFactory.getLogger(TestInstrumentedLock.class);

  @Rule public TestName name = new TestName();

  /**
   * Test exclusive access of the lock.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testMultipleThread() throws Exception {
    String testname = name.getMethodName();
    InstrumentedLock lock = new InstrumentedLock(testname, LOG, 0, 300);
    lock.lock();
    try {
      Thread competingThread = new Thread() {
        @Override
        public void run() {
          assertFalse(lock.tryLock());
        }
      };
      competingThread.start();
      competingThread.join();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Test the correctness with try-with-resource syntax.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testTryWithResourceSyntax() throws Exception {
    String testname = name.getMethodName();
    final AtomicReference<Thread> lockThread = new AtomicReference<>(null);
    Lock lock = new InstrumentedLock(testname, LOG, 0, 300) {
      @Override
      public void lock() {
        super.lock();
        lockThread.set(Thread.currentThread());
      }
      @Override
      public void unlock() {
        super.unlock();
        lockThread.set(null);
      }
    };
    AutoCloseableLock acl = new AutoCloseableLock(lock);
    try (AutoCloseable localLock = acl.acquire()) {
      assertEquals(acl, localLock);
      Thread competingThread = new Thread() {
        @Override
        public void run() {
          assertNotEquals(Thread.currentThread(), lockThread.get());
          assertFalse(lock.tryLock());
        }
      };
      competingThread.start();
      competingThread.join();
      assertEquals(Thread.currentThread(), lockThread.get());
    }
    assertNull(lockThread.get());
  }

  /**
   * Test the lock logs warning when lock held time is greater than threshold
   * and not log warning otherwise.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testLockLongHoldingReport() throws Exception {
    String testname = name.getMethodName();
    final AtomicLong time = new AtomicLong(0);
    Timer mclock = new Timer() {
      @Override
      public long monotonicNow() {
        return time.get();
      }
    };
    Lock mlock = mock(Lock.class);

    final AtomicLong wlogged = new AtomicLong(0);
    final AtomicLong wsuppresed = new AtomicLong(0);
    final AtomicLong wMaxWait = new AtomicLong(0);
    InstrumentedLock lock = new InstrumentedLock(
        testname, LOG, mlock, 2000, 300, mclock) {
      @Override
      void logWarning(long lockHeldTime, SuppressedSnapshot stats) {
        wlogged.incrementAndGet();
        wsuppresed.set(stats.getSuppressedCount());
        wMaxWait.set(stats.getMaxSuppressedWait());
      }
    };

    // do not log warning when the lock held time is short
    lock.lock();   // t = 0
    time.set(200);
    lock.unlock(); // t = 200
    assertEquals(0, wlogged.get());
    assertEquals(0, wsuppresed.get());
    assertEquals(0, wMaxWait.get());

    lock.lock();   // t = 200
    time.set(700);
    lock.unlock(); // t = 700
    assertEquals(1, wlogged.get());
    assertEquals(0, wsuppresed.get());
    assertEquals(0, wMaxWait.get());

    // despite the lock held time is greater than threshold
    // suppress the log warning due to the logging gap
    // (not recorded in wsuppressed until next log message)
    lock.lock();   // t = 700
    time.set(1100);
    lock.unlock(); // t = 1100
    assertEquals(1, wlogged.get());
    assertEquals(0, wsuppresed.get());
    assertEquals(0, wMaxWait.get());

    // log a warning message when the lock held time is greater the threshold
    // and the logging time gap is satisfied. Also should display suppressed
    // previous warnings.
    time.set(2400);
    lock.lock();   // t = 2400
    time.set(2800);
    lock.unlock(); // t = 2800
    assertEquals(2, wlogged.get());
    assertEquals(1, wsuppresed.get());
    assertEquals(400, wMaxWait.get());
  }

  /**
   * Test the lock logs warning when lock wait / queue time is greater than
   * threshold and not log warning otherwise.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testLockLongWaitReport() throws Exception {
    String testname = name.getMethodName();
    final AtomicLong time = new AtomicLong(0);
    Timer mclock = new Timer() {
      @Override
      public long monotonicNow() {
        return time.get();
      }
    };
    Lock mlock = new ReentrantLock(true); //mock(Lock.class);

    final AtomicLong wlogged = new AtomicLong(0);
    final AtomicLong wsuppresed = new AtomicLong(0);
    final AtomicLong wMaxWait = new AtomicLong(0);
    InstrumentedLock lock = new InstrumentedLock(
        testname, LOG, mlock, 2000, 300, mclock) {
      @Override
      void logWaitWarning(long lockHeldTime, SuppressedSnapshot stats) {
        wlogged.incrementAndGet();
        wsuppresed.set(stats.getSuppressedCount());
        wMaxWait.set(stats.getMaxSuppressedWait());
      }
    };

    // do not log warning when the lock held time is short
    lock.lock();   // t = 0

    Thread competingThread = lockUnlockThread(lock);
    time.set(200);
    lock.unlock(); // t = 200
    competingThread.join();
    assertEquals(0, wlogged.get());
    assertEquals(0, wsuppresed.get());
    assertEquals(0, wMaxWait.get());


    lock.lock();   // t = 200
    competingThread = lockUnlockThread(lock);
    time.set(700);
    lock.unlock(); // t = 700
    competingThread.join();

    // The competing thread will have waited for 500ms, so it should log
    assertEquals(1, wlogged.get());
    assertEquals(0, wsuppresed.get());
    assertEquals(0, wMaxWait.get());

    // despite the lock wait time is greater than threshold
    // suppress the log warning due to the logging gap
    // (not recorded in wsuppressed until next log message)
    lock.lock();   // t = 700
    competingThread = lockUnlockThread(lock);
    time.set(1100);
    lock.unlock(); // t = 1100
    competingThread.join();
    assertEquals(1, wlogged.get());
    assertEquals(0, wsuppresed.get());
    assertEquals(0, wMaxWait.get());

    // log a warning message when the lock held time is greater the threshold
    // and the logging time gap is satisfied. Also should display suppressed
    // previous warnings.
    time.set(2400);
    lock.lock();   // t = 2400
    competingThread = lockUnlockThread(lock);
    time.set(2800);
    lock.unlock(); // t = 2800
    competingThread.join();
    assertEquals(2, wlogged.get());
    assertEquals(1, wsuppresed.get());
    assertEquals(400, wMaxWait.get());
  }

  private Thread lockUnlockThread(Lock lock) throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      try {
        assertFalse(lock.tryLock());
        countDownLatch.countDown();
        lock.lock();
      } finally {
        lock.unlock();
      }
    });
    t.start();
    countDownLatch.await();
    // Even with the countdown latch, the main thread releases the lock
    // before this thread actually starts waiting on it, so introducing a
    // short sleep so the competing thread can block on the lock as intended.
    Thread.sleep(3);
    return t;
  }

}
