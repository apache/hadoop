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
import java.net.HttpURLConnection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableScheduledFuture;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;

import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_ACQUIRING_LEASE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_FUTURE_EXISTS;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_NO_LEASE_THREADS;

/**
 * An Azure blob lease that automatically renews itself indefinitely by scheduling lease
 * operations through the ABFS client. Use it to prevent writes to the blob by other processes
 * that don't have the lease.
 *
 * Creating a new Lease object blocks the caller until the Azure blob lease is acquired. It will
 * retry a fixed number of times before failing if there is a problem acquiring the lease.
 *
 * Call free() to release the Lease. If the holder process dies, the lease will time out since it
 * won't be renewed.
 */
public final class SelfRenewingLease {
  private static final Logger LOG = LoggerFactory.getLogger(SelfRenewingLease.class);

  static final int LEASE_DURATION = 60; // Lease duration in seconds
  static final int LEASE_RENEWAL_PERIOD = 40; // Lease renewal interval in seconds

  static final int LEASE_ACQUIRE_RETRY_INTERVAL = 10; // Retry interval for acquiring lease in secs
  static final int LEASE_ACQUIRE_MAX_RETRIES = 7; // Number of retries for acquiring lease

  private final AbfsClient client;
  private final String path;

  // Lease status variables
  private volatile boolean leaseFreed;
  private volatile String leaseID = null;
  private volatile Throwable exception = null;
  private volatile ListenableScheduledFuture<AbfsRestOperation> future = null;

  public static class LeaseException extends AzureBlobFileSystemException {
    public LeaseException(Throwable t) {
      super(ERR_ACQUIRING_LEASE, t);
    }

    public LeaseException(String s) {
      super(s);
    }
  }

  public SelfRenewingLease(AbfsClient client, Path path) throws AzureBlobFileSystemException {
    this.leaseFreed = false;
    this.client = client;
    this.path = getRelativePath(path);

    if (client.getNumLeaseThreads() < 1) {
      throw new LeaseException(ERR_NO_LEASE_THREADS);
    }

    // Try to get the lease a specified number of times, else throw an error
    RetryPolicy retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        LEASE_ACQUIRE_MAX_RETRIES, LEASE_ACQUIRE_RETRY_INTERVAL, TimeUnit.SECONDS);
    acquireLease(retryPolicy, 0, 0);

    while (leaseID == null && exception == null) {
    }
    if (exception != null) {
      LOG.error("Failed to acquire lease on {}", path);
      throw new LeaseException(exception);
    }

    renewLease(LEASE_RENEWAL_PERIOD);

    LOG.debug("Acquired lease {} on {}", leaseID, path);
  }

  private void acquireLease(RetryPolicy retryPolicy, int numRetries, long delay)
      throws LeaseException {
    LOG.debug("Attempting to acquire lease on {}, retry {}", path, numRetries);
    if (future != null && !future.isDone()) {
      throw new LeaseException(ERR_LEASE_FUTURE_EXISTS);
    }
    future = client.schedule(() -> client.acquireLease(path, LEASE_DURATION),
        delay, TimeUnit.SECONDS);
    client.addCallback(future, new FutureCallback<AbfsRestOperation>() {
      @Override
      public void onSuccess(@Nullable AbfsRestOperation op) {
        leaseID = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_LEASE_ID);
        LOG.debug("Acquired lease {} on {}", leaseID, path);
      }

      @Override
      public void onFailure(Throwable throwable) {
        try {
          if (RetryPolicy.RetryAction.RetryDecision.RETRY
              == retryPolicy.shouldRetry(null, numRetries, 0, true).action) {
            LOG.debug("Failed acquire lease on {}, retrying: {}", path, throwable);
            acquireLease(retryPolicy, numRetries + 1, LEASE_ACQUIRE_RETRY_INTERVAL);
          } else {
            exception = throwable;
          }
        } catch (Exception e) {
          exception = throwable;
        }
      }
    });
  }

  private void renewLease(long delay) {
    LOG.debug("Attempting to renew lease on {}, renew lease id {}, delay {}", path, leaseID, delay);
    if (future != null && !future.isDone()) {
      LOG.warn("Unexpected new lease renewal operation occurred while operation already existed. "
          + "Not initiating new renewal");
      return;
    }
    future = client.schedule(() -> client.renewLease(path, leaseID), delay,
            TimeUnit.SECONDS);
    client.addCallback(future, new FutureCallback<AbfsRestOperation>() {
      @Override
      public void onSuccess(@Nullable AbfsRestOperation op) {
        LOG.debug("Renewed lease {} on {}", leaseID, path);
        renewLease(delay);
      }

      @Override
      public void onFailure(Throwable throwable) {
        if (throwable instanceof CancellationException) {
          LOG.info("Stopping renewal due to cancellation");
          free();
          return;
        } else if (throwable instanceof AbfsRestOperationException) {
          AbfsRestOperationException opEx = ((AbfsRestOperationException) throwable);
          if (opEx.getStatusCode() < HttpURLConnection.HTTP_INTERNAL_ERROR) {
            // error in 400 range indicates a type of error that should not result in a retry
            // such as the lease being broken or a different lease being present
            LOG.info("Stopping renewal due to {}: {}, {}", opEx.getStatusCode(),
                opEx.getErrorCode(), opEx.getErrorMessage());
            free();
            return;
          }
        }

        LOG.debug("Failed to renew lease on {}, renew lease id {}, retrying: {}", path, leaseID,
            throwable);
        renewLease(0);
      }
    });
  }

  /**
   * Cancel renewal and free the lease. If an exception occurs, this method assumes the lease
   * will expire after the lease duration.
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
      client.releaseLease(path, leaseID);
    } catch (IOException e) {
      LOG.info("Exception when trying to release lease {} on {}. Lease will be left to expire: {}",
          leaseID, path, e.getMessage());
    } finally {
      // Even if releasing the lease fails (e.g. because the file was deleted),
      // make sure to record that we freed the lease
      leaseFreed = true;
      LOG.debug("Freed lease {} on {}", leaseID, path);
    }
  }

  public boolean isFreed() {
    return leaseFreed;
  }

  public String getLeaseID() {
    return leaseID;
  }

  private String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    return path.toUri().getPath();
  }
}
