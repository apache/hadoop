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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_ACQUIRING_LEASE;

/**
 * An Azure blob lease that automatically renews itself indefinitely
 * using a background thread. Use it to synchronize distributed processes,
 * or to prevent writes to the blob by other processes that don't
 * have the lease.
 *
 * Creating a new Lease object blocks the caller until the Azure blob lease is
 * acquired.
 *
 * Call free() to release the Lease.
 *
 * You can use this Lease like a distributed lock. If the holder process
 * dies, the lease will time out since it won't be renewed.
 *
 * See also {@link org.apache.hadoop.fs.azure.SelfRenewingLease}.
 */
public class SelfRenewingLease {

  private final AbfsClient client;
  private final Path path;
  private Thread renewer;
  private volatile boolean leaseFreed;
  private String leaseID = null;
  private static final int LEASE_TIMEOUT = 60;  // Lease timeout in seconds

  // Time to wait to renew lease in milliseconds
  public static final int LEASE_RENEWAL_PERIOD = 40000;
  public static final Logger LOG = LoggerFactory.getLogger(SelfRenewingLease.class);

  // Used to allocate thread serial numbers in thread name
  private static AtomicInteger threadNumber = new AtomicInteger(0);


  // Time to wait to retry getting the lease in milliseconds
  static final int LEASE_ACQUIRE_RETRY_INTERVAL = 2000;
  static final int LEASE_MAX_RETRIES = 5;

  public static class LeaseException extends AzureBlobFileSystemException {
    public LeaseException(Exception innerException) {
      super(ERR_ACQUIRING_LEASE, innerException);
    }
  }

  public SelfRenewingLease(AbfsClient client, Path path) throws AzureBlobFileSystemException {

    this.leaseFreed = false;
    this.client = client;
    this.path = path;

    // Try to get the lease a specified number of times, else throw an error
    int numRetries = 0;
    while (leaseID == null && numRetries < LEASE_MAX_RETRIES) {
      numRetries++;
      try {
        LOG.debug("lease path: {}", path);
        final AbfsRestOperation op =
            client.acquireLease(getRelativePath(path),
                LEASE_TIMEOUT);

        leaseID = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_LEASE_ID);
      } catch (IOException e) {
        if (numRetries < LEASE_MAX_RETRIES) {
          LOG.info("Caught exception when trying to acquire lease on blob {}, retrying: {}", path,
              e.getMessage());
          LOG.debug("Exception acquiring lease", e);
        } else {
          throw new LeaseException(e);
        }
      }
      if (leaseID == null) {
        try {
          Thread.sleep(LEASE_ACQUIRE_RETRY_INTERVAL);
        } catch (InterruptedException e) {

          // Restore the interrupted status
          Thread.currentThread().interrupt();
        }
      }
    }
    renewer = new Thread(new Renewer());

    // A Renewer running should not keep JVM from exiting, so make it a daemon.
    renewer.setDaemon(true);
    renewer.setName("AzureBFSLeaseRenewer-" + threadNumber.getAndIncrement());
    renewer.start();
    LOG.debug("Acquired lease {} on {} managed by thread {}", leaseID, path, renewer.getName());
  }

  /**
   * Free the lease and stop the keep-alive thread.
   */
  public void free() {
    try {
      LOG.debug("lease path: {}, release lease id: {}", path, leaseID);
      client.releaseLease(getRelativePath(path), leaseID);
    } catch (IOException e) {
      LOG.info("Exception when trying to release lease {} on {}. Lease will be left to expire: {}",
          leaseID, path, e.getMessage());
      LOG.debug("Exception releasing lease", e);
    } finally {

      // Even if releasing the lease fails (e.g. because the file was deleted),
      // make sure to record that we freed the lease, to terminate the
      // keep-alive thread.
      leaseFreed = true;
      LOG.debug("Freed lease {} on {} managed by thread {}", leaseID, path, renewer.getName());
    }
  }

  public boolean isFreed() {
    return leaseFreed;
  }

  public String getLeaseID() {
    return leaseID;
  }

  private class Renewer implements Runnable {

    /**
     * Start a keep-alive thread that will continue to renew
     * the lease until it is freed or the process dies.
     */
    @Override
    public void run() {
      LOG.debug("Starting lease keep-alive thread.");

      while (!leaseFreed) {
        try {
          Thread.sleep(LEASE_RENEWAL_PERIOD);
        } catch (InterruptedException e) {
          LOG.debug("Keep-alive thread for lease {} interrupted", leaseID);

          // Restore the interrupted status
          Thread.currentThread().interrupt();
        }
        try {
          if (!leaseFreed) {
            LOG.debug("lease path: {}, renew lease id: {}", path, leaseID);
            client.renewLease(getRelativePath(path), leaseID);

            // It'll be very rare to renew the lease (most will be short)
            // so log that we did it, to help with system debugging.
            LOG.info("Renewed lease {} on {}", leaseID, path);
          }
        } catch (IOException e) {
          if (!leaseFreed) {

            // Free the lease so we don't leave this thread running forever.
            leaseFreed = true;

            // Normally leases should be freed and there should be no
            // exceptions, so log a warning.
            LOG.warn("Attempt to renew lease {} on {} failed, stopping renewal thread: ",
                leaseID, path, e.getMessage());
            LOG.debug("Exception renewing lease", e);
          }
        }
      }
    }
  }

  private String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    return path.toUri().getPath();
  }
}
