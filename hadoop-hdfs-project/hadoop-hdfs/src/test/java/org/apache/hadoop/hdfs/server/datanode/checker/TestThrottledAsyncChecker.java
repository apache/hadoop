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

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.FakeTimer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Verify functionality of {@link ThrottledAsyncChecker}.
 */
public class TestThrottledAsyncChecker {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestThrottledAsyncChecker.class);
  private static final long MIN_ERROR_CHECK_GAP = 1000;

  /**
   * Test various scheduling combinations to ensure scheduling and
   * throttling behave as expected.
   */
  @Test(timeout=60000)
  public void testScheduler() throws Exception {
    final NoOpCheckable target1 = new NoOpCheckable();
    final NoOpCheckable target2 = new NoOpCheckable();
    final FakeTimer timer = new FakeTimer();
    ThrottledAsyncChecker<Boolean, Boolean> checker =
        new ThrottledAsyncChecker<>(timer, MIN_ERROR_CHECK_GAP,
                                    getExecutorService());

    // check target1 and ensure we get back the expected result.
    assertTrue(checker.schedule(target1, true).isPresent());
    waitTestCheckableCheckCount(target1, 1L);

    // Check target1 again without advancing the timer. target1 should not
    // be checked again.
    assertFalse(checker.schedule(target1, true).isPresent());
    waitTestCheckableCheckCount(target1, 1L);

    // Schedule target2 scheduled without advancing the timer.
    // target2 should be checked as it has never been checked before.
    assertTrue(checker.schedule(target2, true).isPresent());
    waitTestCheckableCheckCount(target2, 1L);

    // Advance the timer but just short of the min gap.
    // Neither target1 nor target2 should be checked again.
    timer.advance(MIN_ERROR_CHECK_GAP - 1);
    assertFalse(checker.schedule(target1, true).isPresent());
    waitTestCheckableCheckCount(target1, 1L);
    assertFalse(checker.schedule(target2, true).isPresent());
    waitTestCheckableCheckCount(target2, 1L);

    // Advance the timer again.
    // Both targets should be checked now.
    timer.advance(MIN_ERROR_CHECK_GAP);
    assertTrue(checker.schedule(target1, true).isPresent());
    waitTestCheckableCheckCount(target1, 2L);
    assertTrue(checker.schedule(target2, true).isPresent());
    waitTestCheckableCheckCount(target2, 2L);
  }

  @Test (timeout=60000)
  public void testCancellation() throws Exception {
    LatchedCheckable target = new LatchedCheckable();
    final FakeTimer timer = new FakeTimer();
    final LatchedCallback callback = new LatchedCallback(target);
    ThrottledAsyncChecker<Boolean, Boolean> checker =
        new ThrottledAsyncChecker<>(timer, MIN_ERROR_CHECK_GAP,
                                    getExecutorService());

    Optional<ListenableFuture<Boolean>> olf =
        checker.schedule(target, true);
    if (olf.isPresent()) {
      Futures.addCallback(olf.get(), callback);
    }

    // Request immediate cancellation.
    checker.shutdownAndWait(0, TimeUnit.MILLISECONDS);
    try {
      assertFalse(olf.get().get());
      fail("Failed to get expected InterruptedException");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof InterruptedException);
    }
    callback.failureLatch.await();
  }

  @Test (timeout=60000)
  public void testConcurrentChecks() throws Exception {
    LatchedCheckable target = new LatchedCheckable();
    final FakeTimer timer = new FakeTimer();
    ThrottledAsyncChecker<Boolean, Boolean> checker =
        new ThrottledAsyncChecker<>(timer, MIN_ERROR_CHECK_GAP,
                                    getExecutorService());
    final Optional<ListenableFuture<Boolean>> olf1 =
        checker.schedule(target, true);

    final Optional<ListenableFuture<Boolean>> olf2 =
        checker.schedule(target, true);

    // Ensure that concurrent requests return the future object
    // for the first caller.
    assertTrue(olf1.isPresent());
    assertFalse(olf2.isPresent());

    // Unblock the latch and wait for it to finish execution.
    target.latch.countDown();
    olf1.get().get();

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        // We should get an absent Optional.
        // This can take a short while until the internal callback in
        // ThrottledAsyncChecker is scheduled for execution.
        // Also this should not trigger a new check operation as the timer
        // was not advanced. If it does trigger a new check then the test
        // will fail with a timeout.
        final Optional<ListenableFuture<Boolean>> olf3 =
            checker.schedule(target, true);
        return !olf3.isPresent();
      }
    }, 100, 10000);
  }

  /**
   * Ensure that the context is passed through to the Checkable#check
   * method.
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testContextIsPassed() throws Exception {
    final NoOpCheckable target1 = new NoOpCheckable();
    final FakeTimer timer = new FakeTimer();
    ThrottledAsyncChecker<Boolean, Boolean> checker =
        new ThrottledAsyncChecker<>(timer, MIN_ERROR_CHECK_GAP,
            getExecutorService());

    assertTrue(checker.schedule(target1, true).isPresent());
    waitTestCheckableCheckCount(target1, 1L);
    timer.advance(MIN_ERROR_CHECK_GAP + 1);
    assertTrue(checker.schedule(target1, false).isPresent());
    waitTestCheckableCheckCount(target1, 2L);

  }

  private void waitTestCheckableCheckCount(TestCheckableBase target,
      long expectedChecks) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        // This can take a short while until the internal callback in
        // ThrottledAsyncChecker is scheduled for execution.
        // If it does trigger a new check then the test
        // will fail with a timeout.
        return target.getTotalChecks() == expectedChecks;
      }
    }, 100, 10000);
  }
  /**
   * Ensure that the exception from a failed check is cached
   * and returned without re-running the check when the minimum
   * gap has not elapsed.
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testExceptionCaching() throws Exception {
    final ThrowingCheckable target1 = new ThrowingCheckable();
    final FakeTimer timer = new FakeTimer();
    ThrottledAsyncChecker<Boolean, Boolean> checker =
        new ThrottledAsyncChecker<>(timer, MIN_ERROR_CHECK_GAP,
            getExecutorService());

    assertTrue(checker.schedule(target1, true).isPresent());
    waitTestCheckableCheckCount(target1, 1L);

    assertFalse(checker.schedule(target1, true).isPresent());
    waitTestCheckableCheckCount(target1, 1L);
  }

  /**
   * A simple ExecutorService for testing.
   */
  private ExecutorService getExecutorService() {
    return new ScheduledThreadPoolExecutor(1);
  }

  private abstract static class TestCheckableBase
      implements Checkable<Boolean, Boolean> {
    protected final AtomicLong numChecks = new AtomicLong(0);

    public long getTotalChecks() {
      return numChecks.get();
    }

    public void incrTotalChecks() {
      numChecks.incrementAndGet();
    }
  }

  /**
   * A Checkable that just returns its input.
   */
  private static class NoOpCheckable
      extends TestCheckableBase {
    @Override
    public Boolean check(Boolean context) {
      incrTotalChecks();
      return context;
    }
  }

  private static class ThrowingCheckable
      extends TestCheckableBase {
    @Override
    public Boolean check(Boolean context) throws DummyException {
      incrTotalChecks();
      throw new DummyException();
    }
  }

  private static class DummyException extends Exception {
  }

  /**
   * A checkable that hangs until signaled.
   */
  private static class LatchedCheckable
      implements Checkable<Boolean, Boolean> {
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public Boolean check(Boolean ignored) throws InterruptedException {
      LOG.info("LatchedCheckable {} waiting.", this);
      latch.await();
      return true;  // Unreachable.
    }
  }

  /**
   * A {@link FutureCallback} that counts its invocations.
   */
  private static final class LatchedCallback
      implements FutureCallback<Boolean> {
    private final CountDownLatch successLatch = new CountDownLatch(1);
    private final CountDownLatch failureLatch = new CountDownLatch(1);
    private final Checkable target;

    private LatchedCallback(Checkable target) {
      this.target = target;
    }

    @Override
    public void onSuccess(@Nonnull Boolean result) {
      LOG.info("onSuccess callback invoked for {}", target);
      successLatch.countDown();
    }

    @Override
    public void onFailure(@Nonnull Throwable t) {
      LOG.info("onFailure callback invoked for {} with exception", target, t);
      failureLatch.countDown();
    }
  }
}
