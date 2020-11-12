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

package org.apache.hadoop.fs.azure;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlobWrapper;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;

import java.util.concurrent.atomic.AtomicInteger;

import static com.microsoft.azure.storage.StorageErrorCodeStrings.LEASE_ALREADY_PRESENT;

/**
 * An Azure blob lease that automatically renews itself indefinitely
 * using a background thread. Use it to synchronize distributed processes,
 * or to prevent writes to the blob by other processes that don't
 * have the lease.
 *
 * Creating a new Lease object blocks the caller until the Azure blob lease is
 * acquired.
 *
 * Attempting to get a lease on a non-existent blob throws StorageException.
 *
 * Call free() to release the Lease.
 *
 * You can use this Lease like a distributed lock. If the holder process
 * dies, the lease will time out since it won't be renewed.
 */
public class SelfRenewingLease {

  private CloudBlobWrapper blobWrapper;
  private Thread renewer;
  private volatile boolean leaseFreed;
  private String leaseID = null;
  private static final int LEASE_TIMEOUT = 60;  // Lease timeout in seconds

  // Time to wait to renew lease in milliseconds
  public static final int LEASE_RENEWAL_PERIOD = 40000;
  private static final Log LOG = LogFactory.getLog(SelfRenewingLease.class);

  // Used to allocate thread serial numbers in thread name
  private static AtomicInteger threadNumber = new AtomicInteger(0);


  // Time to wait to retry getting the lease in milliseconds
  @VisibleForTesting
  static final int LEASE_ACQUIRE_RETRY_INTERVAL = 2000;

  public SelfRenewingLease(CloudBlobWrapper blobWrapper, boolean throwIfPresent)
      throws StorageException {

    this.leaseFreed = false;
    this.blobWrapper = blobWrapper;

    // Keep trying to get the lease until you get it.
    CloudBlob blob = blobWrapper.getBlob();
    while(leaseID == null) {
      try {
        leaseID = blob.acquireLease(LEASE_TIMEOUT, null);
      } catch (StorageException e) {

        if (throwIfPresent && e.getErrorCode().equals(LEASE_ALREADY_PRESENT)) {
          throw e;
        }

        // Throw again if we don't want to keep waiting.
        // We expect it to be that the lease is already present,
        // or in some cases that the blob does not exist.
        if (!LEASE_ALREADY_PRESENT.equals(e.getErrorCode())) {
          LOG.info(
            "Caught exception when trying to get lease on blob "
            + blobWrapper.getUri().toString() + ". " + e.getMessage());
          throw e;
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
    renewer.setName("AzureLeaseRenewer-" + threadNumber.getAndIncrement());
    renewer.start();
    LOG.debug("Acquired lease " + leaseID + " on " + blob.getUri()
        + " managed by thread " + renewer.getName());
  }

  /**
   * Free the lease and stop the keep-alive thread.
   * @throws StorageException Thrown when fail to free the lease.
   */
  public void free() throws StorageException {
    AccessCondition accessCondition = AccessCondition.generateEmptyCondition();
    accessCondition.setLeaseID(leaseID);
    try {
      blobWrapper.getBlob().releaseLease(accessCondition);
    } catch (StorageException e) {
      if ("BlobNotFound".equals(e.getErrorCode())) {

        // Don't do anything -- it's okay to free a lease
        // on a deleted file. The delete freed the lease
        // implicitly.
      } else {

        // This error is not anticipated, so re-throw it.
        LOG.warn("Unanticipated exception when trying to free lease " + leaseID
            + " on " +  blobWrapper.getStorageUri());
        throw(e);
      }
    } finally {

      // Even if releasing the lease fails (e.g. because the file was deleted),
      // make sure to record that we freed the lease, to terminate the
      // keep-alive thread.
      leaseFreed = true;
      LOG.debug("Freed lease " + leaseID + " on " + blobWrapper.getUri()
          + " managed by thread " + renewer.getName());
    }
  }

  public boolean isFreed() {
    return leaseFreed;
  }

  public String getLeaseID() {
    return leaseID;
  }

  public CloudBlob getCloudBlob() {
    return blobWrapper.getBlob();
  }

  private class Renewer implements Runnable {

    /**
     * Start a keep-alive thread that will continue to renew
     * the lease until it is freed or the process dies.
     */
    @Override
    public void run() {
      LOG.debug("Starting lease keep-alive thread.");
      AccessCondition accessCondition =
          AccessCondition.generateEmptyCondition();
      accessCondition.setLeaseID(leaseID);

      while(!leaseFreed) {
        try {
          Thread.sleep(LEASE_RENEWAL_PERIOD);
        } catch (InterruptedException e) {
          LOG.debug("Keep-alive thread for lease " + leaseID +
              " interrupted.");

          // Restore the interrupted status
          Thread.currentThread().interrupt();
        }
        try {
          if (!leaseFreed) {
            blobWrapper.getBlob().renewLease(accessCondition);

            // It'll be very rare to renew the lease (most will be short)
            // so log that we did it, to help with system debugging.
            LOG.info("Renewed lease " + leaseID + " on "
                + getCloudBlob().getUri());
          }
        } catch (StorageException e) {
          if (!leaseFreed) {

            // Free the lease so we don't leave this thread running forever.
            leaseFreed = true;

            // Normally leases should be freed and there should be no
            // exceptions, so log a warning.
            LOG.warn("Attempt to renew lease " + leaseID + " on "
                + getCloudBlob().getUri()
                + " failed, but lease not yet freed. Reason: " +
                e.getMessage());
          }
        }
      }
    }
  }
}
