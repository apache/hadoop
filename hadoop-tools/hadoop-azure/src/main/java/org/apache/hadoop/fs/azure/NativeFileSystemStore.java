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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;
import org.apache.hadoop.fs.permission.PermissionStatus;

import com.google.common.annotations.VisibleForTesting;

/**
 * <p>
 * An abstraction for a key-based {@link File} store.
 * </p>
 */
@InterfaceAudience.Private
interface NativeFileSystemStore {

  void initialize(URI uri, Configuration conf, AzureFileSystemInstrumentation instrumentation) throws IOException;

  void storeEmptyFolder(String key, PermissionStatus permissionStatus)
      throws AzureException;

  FileMetadata retrieveMetadata(String key) throws IOException;

  InputStream retrieve(String key) throws IOException;

  InputStream retrieve(String key, long byteRangeStart) throws IOException;

  DataOutputStream storefile(String keyEncoded,
      PermissionStatus permissionStatus,
      String key) throws AzureException;

  boolean isPageBlobKey(String key);

  boolean isAtomicRenameKey(String key);

  /**
   * Returns the file block size.  This is a fake value used for integration
   * of the Azure store with Hadoop.
   * @return The file block size.
   */
  long getHadoopBlockSize();

  void storeEmptyLinkFile(String key, String tempBlobKey,
      PermissionStatus permissionStatus) throws AzureException;

  String getLinkInFileMetadata(String key) throws AzureException;

  FileMetadata[] list(String prefix, final int maxListingCount,
      final int maxListingDepth) throws IOException;

  void changePermissionStatus(String key, PermissionStatus newPermission)
      throws AzureException;

  /**
   * API to delete a blob in the back end azure storage.
   * @param key - key to the blob being deleted.
   * @return return true when delete is successful, false if
   * blob cannot be found or delete is not possible without
   * exception.
   * @throws IOException Exception encountered while deleting in
   * azure storage.
   */
  boolean delete(String key) throws IOException;

  void rename(String srcKey, String dstKey) throws IOException;

  void rename(String srcKey, String dstKey, boolean acquireLease, SelfRenewingLease existingLease)
      throws IOException;

  void rename(String srcKey, String dstKey, boolean acquireLease,
              SelfRenewingLease existingLease, boolean overwriteDestination)
      throws IOException;

  /**
   * Delete all keys with the given prefix. Used for testing.
   *
   * @param prefix prefix of objects to be deleted.
   * @throws IOException Exception encountered while deleting keys.
   */
  @VisibleForTesting
  void purge(String prefix) throws IOException;

  /**
   * Diagnostic method to dump state to the console.
   *
   * @throws IOException Exception encountered while dumping to console.
   */
  void dump() throws IOException;

  void close();

  void updateFolderLastModifiedTime(String key, SelfRenewingLease folderLease)
      throws AzureException;

  void updateFolderLastModifiedTime(String key, Date lastModified,
      SelfRenewingLease folderLease) throws AzureException;

  /**
   * API to delete a blob in the back end azure storage.
   * @param key - key to the blob being deleted.
   * @param lease - Active lease on the blob.
   * @return return true when delete is successful, false if
   * blob cannot be found or delete is not possible without
   * exception.
   * @throws IOException Exception encountered while deleting in
   * azure storage.
   */
  boolean delete(String key, SelfRenewingLease lease) throws IOException;
      
  SelfRenewingLease acquireLease(String key) throws AzureException;

  DataOutputStream retrieveAppendStream(String key, int bufferSize) throws IOException;

  boolean explicitFileExists(String key) throws AzureException;
}
