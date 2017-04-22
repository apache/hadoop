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
package org.apache.hadoop.hdfs.server.datanode.checker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.util.FakeTimer;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class TestThrottledAsyncCheckerTimeout {
  public static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(TestThrottledAsyncCheckerTimeout.class);

  @Rule
  public TestName testName = new TestName();
  @Rule
  public Timeout testTimeout = new Timeout(300_000);

  private static final long DISK_CHECK_TIMEOUT = 10;
  private ReentrantLock lock;

  private ExecutorService getExecutorService() {
    return new ScheduledThreadPoolExecutor(1);
  }

  @Before
  public void initializeLock() {
    lock = new ReentrantLock();
  }

  @Test
  public void testDiskCheckTimeout() throws Exception {
    LOG.info("Executing {}", testName.getMethodName());

    final DummyCheckable target = new DummyCheckable();
    final FakeTimer timer = new FakeTimer();
    ThrottledAsyncChecker<Boolean, Boolean> checker =
        new ThrottledAsyncChecker<>(timer, 0, DISK_CHECK_TIMEOUT,
            getExecutorService());

    // Acquire lock to halt checker. Release after timeout occurs.
    lock.lock();

    final Optional<ListenableFuture<Boolean>> olf = checker
        .schedule(target, true);

    final AtomicLong numCallbackInvocationsSuccess = new AtomicLong(0);
    final AtomicLong numCallbackInvocationsFailure = new AtomicLong(0);

    AtomicBoolean callbackResult = new AtomicBoolean(false);
    final Throwable[] throwable = new Throwable[1];

    assertTrue(olf.isPresent());
    Futures.addCallback(olf.get(), new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(Boolean result) {
        numCallbackInvocationsSuccess.incrementAndGet();
        callbackResult.set(true);
      }

      @Override
      public void onFailure(Throwable t) {
        throwable[0] = t;
        numCallbackInvocationsFailure.incrementAndGet();
        callbackResult.set(true);
      }
    });

    while (!callbackResult.get()) {
      // Wait for the callback
      Thread.sleep(DISK_CHECK_TIMEOUT);
    }

    lock.unlock();

    assertThat(numCallbackInvocationsFailure.get(), is(1L));
    assertThat(numCallbackInvocationsSuccess.get(), is(0L));
    assertTrue(throwable[0] instanceof TimeoutException);
  }

  @Test
  public void testDiskCheckTimeoutInvokesOneCallbackOnly() throws Exception {
    LOG.info("Executing {}", testName.getMethodName());

    final DummyCheckable target = new DummyCheckable();
    final FakeTimer timer = new FakeTimer();
    ThrottledAsyncChecker<Boolean, Boolean> checker =
        new ThrottledAsyncChecker<>(timer, 0, DISK_CHECK_TIMEOUT,
            getExecutorService());
    FutureCallback<Boolean> futureCallback = mock(FutureCallback.class);

    // Acquire lock to halt disk checker. Release after timeout occurs.
    lock.lock();

    final Optional<ListenableFuture<Boolean>> olf1 = checker
        .schedule(target, true);

    assertTrue(olf1.isPresent());
    Futures.addCallback(olf1.get(), futureCallback);

    // Verify that timeout results in only 1 onFailure call and 0 onSuccess
    // calls.
    verify(futureCallback, timeout((int) DISK_CHECK_TIMEOUT*10).times(1))
        .onFailure(any());
    verify(futureCallback, timeout((int) DISK_CHECK_TIMEOUT*10).times(0))
        .onSuccess(any());

    // Release lock so that target can acquire it.
    lock.unlock();

    final Optional<ListenableFuture<Boolean>> olf2 = checker
        .schedule(target, true);

    assertTrue(olf2.isPresent());
    Futures.addCallback(olf2.get(), futureCallback);

    // Verify that normal check (dummy) results in only 1 onSuccess call.
    // Number of times onFailure is invoked should remain the same i.e. 1.
    verify(futureCallback, timeout((int) DISK_CHECK_TIMEOUT*10).times(1))
        .onFailure(any());
    verify(futureCallback, timeout((int) DISK_CHECK_TIMEOUT*10).times(1))
        .onSuccess(any());
  }

  @Test
  public void testTimeoutExceptionIsNotThrownForGoodDisk() throws Exception {
    LOG.info("Executing {}", testName.getMethodName());

    final DummyCheckable target = new DummyCheckable();
    final FakeTimer timer = new FakeTimer();
    ThrottledAsyncChecker<Boolean, Boolean> checker =
        new ThrottledAsyncChecker<>(timer, 0, DISK_CHECK_TIMEOUT,
            getExecutorService());

    final Optional<ListenableFuture<Boolean>> olf = checker
        .schedule(target, true);

    AtomicBoolean callbackResult = new AtomicBoolean(false);
    final Throwable[] throwable = new Throwable[1];

    assertTrue(olf.isPresent());
    Futures.addCallback(olf.get(), new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(Boolean result) {
        callbackResult.set(true);
      }

      @Override
      public void onFailure(Throwable t) {
        throwable[0] = t;
        callbackResult.set(true);
      }
    });

    while (!callbackResult.get()) {
      // Wait for the callback
      Thread.sleep(DISK_CHECK_TIMEOUT);
    }

    assertTrue(throwable[0] == null);
  }

  /**
   * A dummy Checkable that just returns true after acquiring lock.
   */
  protected class DummyCheckable implements Checkable<Boolean,Boolean> {

    @Override
    public Boolean check(Boolean context) throws Exception {
      // Wait to acquire lock
      lock.lock();
      lock.unlock();
      return true;
    }
  }
}
