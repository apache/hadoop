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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import org.junit.Test;

import com.microsoft.azure.storage.StorageException;

/*
 * Tests the Native Azure file system (WASB) against an actual blob store if
 * provided in the environment.
 */
public class TestNativeAzureFileSystemLive extends
    NativeAzureFileSystemBaseTest {

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }

  /**
   * Tests fs.delete() function to delete a blob when another blob is holding a
   * lease on it. Delete if called without a lease should fail if another process
   * is holding a lease and throw appropriate exception
   * This is a scenario that would happen in HMaster startup when it tries to
   * clean up the temp dirs while the HMaster process which was killed earlier
   * held lease on the blob when doing some DDL operation
   */
  @Test
  public void testDeleteThrowsExceptionWithLeaseExistsErrorMessage()
      throws Exception {
    LOG.info("Starting test");
    final String FILE_KEY = "fileWithLease";
    // Create the file
    Path path = new Path(FILE_KEY);
    fs.create(path);
    assertTrue(fs.exists(path));
    NativeAzureFileSystem nfs = (NativeAzureFileSystem)fs;
    final String fullKey = nfs.pathToKey(nfs.makeAbsolute(path));
    final AzureNativeFileSystemStore store = nfs.getStore();

    // Acquire the lease on the file in a background thread
    final CountDownLatch leaseAttemptComplete = new CountDownLatch(1);
    final CountDownLatch beginningDeleteAttempt = new CountDownLatch(1);
    Thread t = new Thread() {
      @Override
      public void run() {
        // Acquire the lease and then signal the main test thread.
        SelfRenewingLease lease = null;
        try {
          lease = store.acquireLease(fullKey);
          LOG.info("Lease acquired: " + lease.getLeaseID());
        } catch (AzureException e) {
          LOG.warn("Lease acqusition thread unable to acquire lease", e);
        } finally {
          leaseAttemptComplete.countDown();
        }

        // Wait for the main test thread to signal it will attempt the delete.
        try {
          beginningDeleteAttempt.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        // Keep holding the lease past the lease acquisition retry interval, so
        // the test covers the case of delete retrying to acquire the lease.
        try {
          Thread.sleep(SelfRenewingLease.LEASE_ACQUIRE_RETRY_INTERVAL * 3);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }

        try {
          if (lease != null){
            LOG.info("Freeing lease");
            lease.free();
          }
        } catch (StorageException se) {
          LOG.warn("Unable to free lease.", se);
        }
      }
    };

    // Start the background thread and wait for it to signal the lease is held.
    t.start();
    try {
      leaseAttemptComplete.await();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    // Try to delete the same file
    beginningDeleteAttempt.countDown();
    store.delete(fullKey);

    // At this point file SHOULD BE DELETED
    assertFalse(fs.exists(path));
  }

  /**
   * Check that isPageBlobKey works as expected. This assumes that
   * in the test configuration, the list of supported page blob directories
   * only includes "pageBlobs". That's why this test is made specific
   * to this subclass.
   */
  @Test
  public void testIsPageBlobKey() {
    AzureNativeFileSystemStore store = ((NativeAzureFileSystem) fs).getStore();

    // Use literal strings so it's easier to understand the tests.
    // In case the constant changes, we want to know about it so we can update this test.
    assertEquals(AzureBlobStorageTestAccount.DEFAULT_PAGE_BLOB_DIRECTORY, "pageBlobs");

    // URI prefix for test environment.
    String uriPrefix = "file:///";

    // negative tests
    String[] negativeKeys = { "", "/", "bar", "bar/", "bar/pageBlobs", "bar/pageBlobs/foo",
        "bar/pageBlobs/foo/", "/pageBlobs/", "/pageBlobs", "pageBlobs", "pageBlobsxyz/" };
    for (String s : negativeKeys) {
      assertFalse(store.isPageBlobKey(s));
      assertFalse(store.isPageBlobKey(uriPrefix + s));
    }

    // positive tests
    String[] positiveKeys = { "pageBlobs/", "pageBlobs/foo/", "pageBlobs/foo/bar/" };
    for (String s : positiveKeys) {
      assertTrue(store.isPageBlobKey(s));
      assertTrue(store.isPageBlobKey(uriPrefix + s));
    }
  }

  /**
   * Test that isAtomicRenameKey() works as expected.
   */
  @Test
  public void testIsAtomicRenameKey() {

    AzureNativeFileSystemStore store = ((NativeAzureFileSystem) fs).getStore();

    // We want to know if the default configuration changes so we can fix
    // this test.
    assertEquals(AzureBlobStorageTestAccount.DEFAULT_ATOMIC_RENAME_DIRECTORIES,
        "/atomicRenameDir1,/atomicRenameDir2");

    // URI prefix for test environment.
    String uriPrefix = "file:///";

    // negative tests
    String[] negativeKeys = { "", "/", "bar", "bar/", "bar/hbase",
        "bar/hbase/foo", "bar/hbase/foo/", "/hbase/", "/hbase", "hbase",
        "hbasexyz/", "foo/atomicRenameDir1/"};
    for (String s : negativeKeys) {
      assertFalse(store.isAtomicRenameKey(s));
      assertFalse(store.isAtomicRenameKey(uriPrefix + s));
    }

    // Positive tests. The directories for atomic rename are /hbase
    // plus the ones in the configuration (DEFAULT_ATOMIC_RENAME_DIRECTORIES
    // for this test).
    String[] positiveKeys = { "hbase/", "hbase/foo/", "hbase/foo/bar/",
        "atomicRenameDir1/foo/", "atomicRenameDir2/bar/"};
    for (String s : positiveKeys) {
      assertTrue(store.isAtomicRenameKey(s));
      assertTrue(store.isAtomicRenameKey(uriPrefix + s));
    }
  }

  /**
   * Tests fs.mkdir() function to create a target blob while another thread
   * is holding the lease on the blob. mkdir should not fail since the blob
   * already exists.
   * This is a scenario that would happen in HBase distributed log splitting.
   * Multiple threads will try to create and update "recovered.edits" folder
   * under the same path.
   */
  @Test
  public void testMkdirOnExistingFolderWithLease() throws Exception {
    SelfRenewingLease lease;
    final String FILE_KEY = "folderWithLease";
    // Create the folder
    fs.mkdirs(new Path(FILE_KEY));
    NativeAzureFileSystem nfs = (NativeAzureFileSystem) fs;
    String fullKey = nfs.pathToKey(nfs.makeAbsolute(new Path(FILE_KEY)));
    AzureNativeFileSystemStore store = nfs.getStore();
    // Acquire the lease on the folder
    lease = store.acquireLease(fullKey);
    assertTrue(lease.getLeaseID() != null);
    // Try to create the same folder
    store.storeEmptyFolder(fullKey,
      nfs.createPermissionStatus(FsPermission.getDirDefault()));
    lease.free();
  }
}
