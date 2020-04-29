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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;

/**
 * <p>
 * Holds basic metadata for a file stored in a {@link NativeFileSystemStore}.
 * </p>
 */
@InterfaceAudience.Private
class FileMetadata extends FileStatus {
  // this is not final so that it can be cleared to save memory when not needed.
  private String key;
  private final BlobMaterialization blobMaterialization;

  /**
   * Constructs a FileMetadata object for a file.
   * 
   * @param key
   *          The key (path) to the file.
   * @param length
   *          The length in bytes of the file.
   * @param lastModified
   *          The last modified date (milliseconds since January 1, 1970 UTC.)
   * @param permissionStatus
   *          The permission for the file.
   * @param blockSize
   *          The Hadoop file block size.
   */
  public FileMetadata(String key, long length, long lastModified,
      PermissionStatus permissionStatus, final long blockSize) {
    super(length, false, 1, blockSize, lastModified, 0,
        permissionStatus.getPermission(),
        permissionStatus.getUserName(),
        permissionStatus.getGroupName(),
        null);
    this.key = key;
    // Files are never implicit.
    this.blobMaterialization = BlobMaterialization.Explicit;
  }

  /**
   * Constructs a FileMetadata object for a directory.
   * 
   * @param key
   *          The key (path) to the directory.
   * @param lastModified
   *          The last modified date (milliseconds since January 1, 1970 UTC.)
   * @param permissionStatus
   *          The permission for the directory.
   * @param blobMaterialization
   *          Whether this is an implicit (no real blob backing it) or explicit
   *          directory.
   * @param blockSize
   *          The Hadoop file block size.
   */
  public FileMetadata(String key, long lastModified,
      PermissionStatus permissionStatus, BlobMaterialization blobMaterialization,
      final long blockSize) {
    super(0, true, 1, blockSize, lastModified, 0,
        permissionStatus.getPermission(),
        permissionStatus.getUserName(),
        permissionStatus.getGroupName(),
        null);
    this.key = key;
    this.blobMaterialization = blobMaterialization;
  }

  @Override
  public Path getPath() {
    Path p = super.getPath();
    if (p == null) {
      // Don't store this yet to reduce memory usage, as it will
      // stay in the Eden Space and later we will update it
      // with the full canonicalized path.
      p = NativeAzureFileSystem.keyToPath(key);
    }
    return p;
  }

  /**
   * Returns the Azure storage key for the file.  Used internally by the framework.
   *
   * @return The key for the file.
   */
  public String getKey() {
    return key;
  }

  /**
   * Indicates whether this is an implicit directory (no real blob backing it)
   * or an explicit one.
   * 
   * @return Implicit if this is an implicit directory, or Explicit if it's an
   *         explicit directory or a file.
   */
  public BlobMaterialization getBlobMaterialization() {
    return blobMaterialization;
  }

  void removeKey() {
    key = null;
  }
}
