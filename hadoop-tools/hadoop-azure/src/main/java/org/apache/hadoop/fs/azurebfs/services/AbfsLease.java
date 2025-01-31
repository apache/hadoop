/**
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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableScheduledFuture;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.INFINITE_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_ACQUIRING_LEASE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_FUTURE_EXISTS;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_NO_LEASE_THREADS;
import static org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation.ONE_THOUSAND;

/**
 * AbfsLease manages an Azure blob lease. It acquires an infinite lease on instantiation and
 * releases the lease when free() is called. Use it to prevent writes to the blob by other
 * processes that don't have the lease.
 *
 * Creating a new Lease object blocks the caller until the Azure blob lease is acquired. It will
 * retry a fixed number of times before failing if there is a problem acquiring the lease.
 *
 * Call free() to release the Lease. If the holder process dies, AzureBlobFileSystem breakLease
 * will need to be called before another client will be able to write to the file.
 */
public final class AbfsLease {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsLease.class);

  // Number of retries for acquiring lease
  static final int DEFAULT_LEASE_ACQUIRE_MAX_RETRIES = 7;
  // Retry interval for acquiring lease in secs
  static final int DEFAULT_LEASE_ACQUIRE_RETRY_INTERVAL = 10;

  private final AbfsClient client;
  private final String path;
  private final TracingContext tracingContext;

  // Lease status variables
  private volatile boolean leaseFreed;
  private volatile String leaseID = null;
  private volatile Throwable exception = null;
  private volatile int acquireRetryCount = 0;
  private volatile ListenableScheduledFuture<AbfsRestOperation> future = null;
  private final long leaseRefreshDuration;
  private final int leaseRefreshDurationInSeconds;
  private final Timer timer;
  private LeaseTimerTask leaseTimerTask;
  private final boolean isAsync;

  public static class LeaseException extends AzureBlobFileSystemException {
    public LeaseException(Throwable t) {
      super(ERR_ACQUIRING_LEASE + ": " + t, t);
    }

    public LeaseException(String s) {
      super(s);
    }
  }

    /**
     * Create a new lease object and acquire a lease on the given path.
     *
     * @param client              AbfsClient
     * @param path                Path to acquire lease on
     * @param isAsync             Whether to acquire lease asynchronously
     * @param leaseRefreshDuration Duration in milliseconds to renew the lease
     * @param eTag                ETag of the file
     * @param tracingContext      Tracing context
     * @throws AzureBlobFileSystemException if the lease cannot be acquired
     */
  public AbfsLease(AbfsClient client, String path,
                   final boolean isAsync, final long leaseRefreshDuration,
                   final String eTag, TracingContext tracingContext) throws AzureBlobFileSystemException {
    this(client, path, isAsync, DEFAULT_LEASE_ACQUIRE_MAX_RETRIES,
            DEFAULT_LEASE_ACQUIRE_RETRY_INTERVAL, leaseRefreshDuration, eTag, tracingContext);
  }

  /**
   * Create a new lease object and acquire a lease on the given path.
   *
   * @param client              AbfsClient
   * @param path                Path to acquire lease on
   * @param isAsync             Whether to acquire lease asynchronously
   * @param acquireMaxRetries   Maximum number of retries to acquire lease
   * @param acquireRetryInterval Retry interval in seconds to acquire lease
   * @param leaseRefreshDuration Duration in milliseconds to renew the lease
   * @param eTag                ETag of the file
   * @param tracingContext      Tracing context
   * @throws AzureBlobFileSystemException if the lease cannot be acquired
   */
  @VisibleForTesting
  public AbfsLease(AbfsClient client, String path, final boolean isAsync, int acquireMaxRetries,
                   int acquireRetryInterval, final long leaseRefreshDuration,
                   final String eTag,
                   TracingContext tracingContext) throws AzureBlobFileSystemException {
    this.leaseFreed = false;
    this.client = client;
    this.path = path;
    this.tracingContext = tracingContext;
    this.leaseRefreshDuration = leaseRefreshDuration;
    this.leaseRefreshDurationInSeconds = (int) leaseRefreshDuration / ONE_THOUSAND;
    this.isAsync = isAsync;

    if (isAsync && client.getNumLeaseThreads() < 1) {
      throw new LeaseException(ERR_NO_LEASE_THREADS);
    }

    // Try to get the lease a specified number of times, else throw an error
    RetryPolicy retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        acquireMaxRetries, acquireRetryInterval, TimeUnit.SECONDS);
    this.timer = new Timer(
            String.format("lease-refresh-timer-%s", path), true);
    acquireLease(retryPolicy, 0, acquireRetryInterval, 0, eTag,
        new TracingContext(tracingContext));

    while (leaseID == null && exception == null) {
      try {
        future.get();
      } catch (Exception e) {
        LOG.debug("Got exception waiting for acquire lease future. Checking if lease ID or "
            + "exception have been set", e);
      }
    }
    if (exception != null) {
      LOG.error("Failed to acquire lease on {}", path);
      throw new LeaseException(exception);
    }

    LOG.debug("Acquired lease {} on {}", leaseID, path);
  }

  /**
   * Acquire a lease on the given path.
   *
   * @param retryPolicy        Retry policy
   * @param numRetries         Number of retries
   * @param retryInterval      Retry interval in seconds
   * @param delay              Delay in seconds
   * @param eTag               ETag of the file
   * @param tracingContext     Tracing context
   * @throws LeaseException if the lease cannot be acquired
   */
  private void acquireLease(RetryPolicy retryPolicy, int numRetries,
      int retryInterval, long delay, final String eTag, TracingContext tracingContext)
      throws LeaseException {
    LOG.debug("Attempting to acquire lease on {}, retry {}", path, numRetries);
    if (future != null && !future.isDone()) {
      throw new LeaseException(ERR_LEASE_FUTURE_EXISTS);
    }
    FutureCallback<AbfsRestOperation> acquireCallback = new FutureCallback<AbfsRestOperation>() {
      @Override
      public void onSuccess(@Nullable AbfsRestOperation op) {
        leaseID = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_LEASE_ID);
        if (leaseRefreshDuration != INFINITE_LEASE_DURATION) {
          leaseTimerTask = new LeaseTimerTask(client, path,
                  leaseID, tracingContext);
          timer.scheduleAtFixedRate(leaseTimerTask, leaseRefreshDuration / 2,
                  leaseRefreshDuration / 2);
        }
        LOG.debug("Acquired lease {} on {}", leaseID, path);
      }

      @Override
      public void onFailure(Throwable throwable) {
        try {
          if (RetryPolicy.RetryAction.RetryDecision.RETRY
              == retryPolicy.shouldRetry(null, numRetries, 0, true).action) {
            LOG.debug("Failed to acquire lease on {}, retrying: {}", path, throwable);
            acquireRetryCount++;
            acquireLease(retryPolicy, numRetries + 1, retryInterval,
                retryInterval, eTag, tracingContext);
          } else {
            exception = throwable;
          }
        } catch (Exception e) {
          exception = throwable;
        }
      }
    };
    if (!isAsync) {
      try {
        AbfsRestOperation op = client.acquireLease(path,
            leaseRefreshDurationInSeconds, eTag, tracingContext);
        acquireCallback.onSuccess(op);
        return;
      } catch (AzureBlobFileSystemException ex) {
        acquireCallback.onFailure(ex);
      }
    }
    future = client.schedule(() -> client.acquireLease(path,
                    INFINITE_LEASE_DURATION, eTag, tracingContext),
            delay, TimeUnit.SECONDS);
    client.addCallback(future, acquireCallback);
  }

  /**
   * Cancel future and free the lease. If an exception occurs while releasing the lease, the error
   * will be logged. If the lease cannot be released, AzureBlobFileSystem breakLease will need to
   * be called before another client will be able to write to the file.
   */
  public void free() {
    if (leaseFreed) {
      return;
    }
    try {
      LOG.debug("Freeing lease: path {}, lease id {}", path, leaseID);
      if (future != null && !future.isDone()) {
        future.cancel(true);
      }
      cancelTimer();
      TracingContext tracingContext = new TracingContext(this.tracingContext);
      tracingContext.setOperation(FSOperationType.RELEASE_LEASE);
      client.releaseLease(path, leaseID, tracingContext);
    } catch (IOException e) {
      LOG.warn("Exception when trying to release lease {} on {}. Lease will need to be broken: {}",
          leaseID, path, e.getMessage());
    } finally {
      // Even if releasing the lease fails (e.g. because the file was deleted),
      // make sure to record that we freed the lease
      leaseFreed = true;
      LOG.debug("Freed lease {} on {}", leaseID, path);
    }
  }

    /**
     * Cancel the lease renewal timer.
     * Also purge the lease refresh timer.
     */
  public void cancelTimer() {
    if (leaseTimerTask != null) {
      leaseTimerTask.cancel();
    }
    timer.purge();
  }

  /**
   * Check if the lease has been freed.
   *
   * @return true if the lease has been freed
   */
  public boolean isFreed() {
    return leaseFreed;
  }

  /**
   * Get the lease ID.
   *
   * @return lease ID
   */
  public String getLeaseID() {
    return leaseID;
  }

  /**
   * Get the number of times the lease was retried.
   *
   * @return number of acquired retry count
   */
  @VisibleForTesting
  public int getAcquireRetryCount() {
    return acquireRetryCount;
  }

  /**
   * Get Tracing Context.
   *
   * @return TracingContext tracing context
   */
  @VisibleForTesting
  public TracingContext getTracingContext() {
    return tracingContext;
  }

  /**
   * Class to track lease renewal.
   * If the lease is not renewed, the lease will expire and the file will be available for write.
   */
  private static class LeaseTimerTask extends TimerTask {
    private final AbfsClient client;
    private final String path;
    private final String leaseID;
    private final TracingContext tracingContext;
    LeaseTimerTask(AbfsClient client, String path, String leaseID, TracingContext tracingContext) {
      this.client = client;
      this.path = path;
      this.leaseID = leaseID;
      this.tracingContext = tracingContext;
    }
    @Override
    public void run() {
      try {
        client.renewLease(path, leaseID, tracingContext);
      } catch (Exception e) {
        LOG.error("Failed to renew lease on {}", path, e);
      }
    }
  }
}
