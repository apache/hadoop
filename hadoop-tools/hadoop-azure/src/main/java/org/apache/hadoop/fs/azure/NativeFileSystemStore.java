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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

  DataInputStream retrieve(String key) throws IOException;

  DataInputStream retrieve(String key, long byteRangeStart) throws IOException;

  DataOutputStream storefile(String key, PermissionStatus permissionStatus)
      throws AzureException;

  boolean isPageBlobKey(String key);

  boolean isAtomicRenameKey(String key);

  void storeEmptyLinkFile(String key, String tempBlobKey,
      PermissionStatus permissionStatus) throws AzureException;

  String getLinkInFileMetadata(String key) throws AzureException;

  PartialListing list(String prefix, final int maxListingCount,
      final int maxListingDepth) throws IOException;

  PartialListing list(String prefix, final int maxListingCount,
      final int maxListingDepth, String priorLastKey) throws IOException;

  PartialListing listAll(String prefix, final int maxListingCount,
      final int maxListingDepth, String priorLastKey) throws IOException;

  void changePermissionStatus(String key, PermissionStatus newPermission)
      throws AzureException;

  void delete(String key) throws IOException;

  void rename(String srcKey, String dstKey) throws IOException;

  void rename(String srcKey, String dstKey, boolean acquireLease, SelfRenewingLease existingLease)
      throws IOException;

  /**
   * Delete all keys with the given prefix. Used for testing.
   *
   * @throws IOException
   */
  @VisibleForTesting
  void purge(String prefix) throws IOException;

  /**
   * Diagnostic method to dump state to the console.
   *
   * @throws IOException
   */
  void dump() throws IOException;

  void close();

  void updateFolderLastModifiedTime(String key, SelfRenewingLease folderLease)
      throws AzureException;

  void updateFolderLastModifiedTime(String key, Date lastModified,
      SelfRenewingLease folderLease) throws AzureException;

  void delete(String key, SelfRenewingLease lease) throws IOException;
      
  SelfRenewingLease acquireLease(String key) throws AzureException;

  DataOutputStream retrieveAppendStream(String key, int bufferSize) throws IOException;

  boolean explicitFileExists(String key) throws AzureException;
}
