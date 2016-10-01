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
package org.apache.hadoop.hdfs;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.FakeTimer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * A test class for {@link InstrumentedReentrantLock}.
 */
public class TestInstrumentedReentrantLock {

  static final Log LOG = LogFactory.getLog(TestInstrumentedReentrantLock.class);

  @Rule public TestName name = new TestName();

  /**
   * Test exclusive access of the lock.
   * @throws Exception
   */
  @Test(timeout=10000)
  public void testMultipleThread() throws Exception {
    String testname = name.getMethodName();
    final InstrumentedReentrantLock lock =
        new InstrumentedReentrantLock(testname, LOG, 0, 300);
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
    final Lock lock = new InstrumentedReentrantLock(testname, LOG, 0, 300) {
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
    FakeTimer mclock = new FakeTimer();
    final int warningThreshold = 500;
    final int minLoggingGap = warningThreshold * 10;

    final AtomicLong wlogged = new AtomicLong(0);
    final AtomicLong wsuppresed = new AtomicLong(0);
    InstrumentedReentrantLock lock = new InstrumentedReentrantLock(
        testname, LOG, new ReentrantLock(), minLoggingGap,
        warningThreshold, mclock) {
      @Override
      void logWarning(long lockHeldTime, long suppressed) {
        wlogged.incrementAndGet();
        wsuppresed.set(suppressed);
      }
    };

    // do not log warning when the lock held time is <= warningThreshold.
    lock.lock();
    mclock.advance(warningThreshold);
    lock.unlock();
    assertEquals(0, wlogged.get());
    assertEquals(0, wsuppresed.get());

    // log a warning when the lock held time exceeds the threshold.
    lock.lock();
    mclock.advance(warningThreshold + 1);
    assertEquals(1, lock.lock.getHoldCount());
    lock.unlock();
    assertEquals(1, wlogged.get());
    assertEquals(0, wsuppresed.get());

    // despite the lock held time is greater than threshold
    // suppress the log warning due to the logging gap
    // (not recorded in wsuppressed until next log message)
    lock.lock();
    mclock.advance(warningThreshold + 1);
    lock.unlock();
    assertEquals(1, wlogged.get());
    assertEquals(0, wsuppresed.get());

    // log a warning message when the lock held time is greater the threshold
    // and the logging time gap is satisfied. Also should display suppressed
    // previous warnings.
    lock.lock();
    mclock.advance(minLoggingGap + 1);
    lock.unlock(); // t = 2800
    assertEquals(2, wlogged.get());
    assertEquals(1, wsuppresed.get());

    // Ensure that nested acquisitions do not log.
    wlogged.set(0);
    wsuppresed.set(0);
    lock.lock();
    lock.lock();
    mclock.advance(minLoggingGap + 1);
    lock.unlock();
    assertEquals(0, wlogged.get());    // No warnings on nested release.
    assertEquals(0, wsuppresed.get());
    lock.unlock();
    assertEquals(1, wlogged.get());    // Last release immediately logs.
    assertEquals(0, wsuppresed.get());
  }
}
