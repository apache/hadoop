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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link AsyncChecker} that skips checking recently
 * checked objects. It will enforce at least {@link minMsBetweenChecks}
 * milliseconds between two successive checks of any one object.
 *
 * It is assumed that the total number of Checkable objects in the system
 * is small, (not more than a few dozen) since the checker uses O(Checkables)
 * storage and also potentially O(Checkables) threads.
 *
 * {@link minMsBetweenChecks} should be configured reasonably
 * by the caller to avoid spinning up too many threads frequently.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ThrottledAsyncChecker<K, V> implements AsyncChecker<K, V> {
  public static final Logger LOG =
      LoggerFactory.getLogger(ThrottledAsyncChecker.class);

  private final Timer timer;

  /**
   * The ExecutorService used to schedule asynchronous checks.
   */
  private final ListeningExecutorService executorService;

  /**
   * The minimum gap in milliseconds between two successive checks
   * of the same object. This is the throttle.
   */
  private final long minMsBetweenChecks;

  /**
   * Map of checks that are currently in progress. Protected by the object
   * lock.
   */
  private final Map<Checkable, ListenableFuture<V>> checksInProgress;

  /**
   * Maps Checkable objects to a future that can be used to retrieve
   * the results of the operation.
   * Protected by the object lock.
   */
  private final Map<Checkable, LastCheckResult<V>> completedChecks;

  ThrottledAsyncChecker(final Timer timer,
                        final long minMsBetweenChecks,
                        final ExecutorService executorService) {
    this.timer = timer;
    this.minMsBetweenChecks = minMsBetweenChecks;
    this.executorService = MoreExecutors.listeningDecorator(executorService);
    this.checksInProgress = new HashMap<>();
    this.completedChecks = new WeakHashMap<>();
  }

  /**
   * See {@link AsyncChecker#schedule}
   *
   * If the object has been checked recently then the check will
   * be skipped. Multiple concurrent checks for the same object
   * will receive the same Future.
   */
  @Override
  public synchronized ListenableFuture<V> schedule(
      final Checkable<K, V> target,
      final K context) {
    LOG.debug("Scheduling a check of {}", target);

    if (checksInProgress.containsKey(target)) {
      return checksInProgress.get(target);
    }

    if (completedChecks.containsKey(target)) {
      final LastCheckResult<V> result = completedChecks.get(target);
      final long msSinceLastCheck = timer.monotonicNow() - result.completedAt;
      if (msSinceLastCheck < minMsBetweenChecks) {
        LOG.debug("Skipped checking {}. Time since last check {}ms " +
            "is less than the min gap {}ms.",
            target, msSinceLastCheck, minMsBetweenChecks);
        return result.result != null ?
            Futures.immediateFuture(result.result) :
            Futures.immediateFailedFuture(result.exception);
      }
    }

    final ListenableFuture<V> lf = executorService.submit(
        new Callable<V>() {
          @Override
          public V call() throws Exception {
            return target.check(context);
          }
        });
    checksInProgress.put(target, lf);
    addResultCachingCallback(target, lf);
    return lf;
  }

  /**
   * Register a callback to cache the result of a check.
   * @param target
   * @param lf
   */
  private void addResultCachingCallback(
      Checkable<K, V> target, ListenableFuture<V> lf) {
    Futures.addCallback(lf, new FutureCallback<V>() {
      @Override
      public void onSuccess(@Nullable V result) {
        synchronized (ThrottledAsyncChecker.this) {
          checksInProgress.remove(target);
          completedChecks.put(target, new LastCheckResult<>(
              result, timer.monotonicNow()));
        }
      }

      @Override
      public void onFailure(@Nonnull Throwable t) {
        synchronized (ThrottledAsyncChecker.this) {
          checksInProgress.remove(target);
          completedChecks.put(target, new LastCheckResult<>(
              t, timer.monotonicNow()));
        }
      }
    });
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  public void shutdownAndWait(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    // Try orderly shutdown.
    executorService.shutdown();

    if (!executorService.awaitTermination(timeout, timeUnit)) {
      // Interrupt executing tasks and wait again.
      executorService.shutdownNow();
      executorService.awaitTermination(timeout, timeUnit);
    }
  }

  /**
   * Status of running a check. It can either be a result or an
   * exception, depending on whether the check completed or threw.
   */
  private static final class LastCheckResult<V> {
    /**
     * Timestamp at which the check completed.
     */
    private final long completedAt;

    /**
     * Result of running the check if it completed. null if it threw.
     */
    @Nullable
    private final V result;

    /**
     * Exception thrown by the check. null if it returned a result.
     */
    private final Throwable exception; // null on success.

    /**
     * Initialize with a result.
     * @param result
     */
    private LastCheckResult(V result, long completedAt) {
      this.result = result;
      this.exception = null;
      this.completedAt = completedAt;
    }

    /**
     * Initialize with an exception.
     * @param completedAt
     * @param t
     */
    private LastCheckResult(Throwable t, long completedAt) {
      this.result = null;
      this.exception = t;
      this.completedAt = completedAt;
    }
  }
}
