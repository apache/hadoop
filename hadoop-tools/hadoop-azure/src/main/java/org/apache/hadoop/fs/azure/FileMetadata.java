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
import org.apache.hadoop.fs.permission.PermissionStatus;

/**
 * <p>
 * Holds basic metadata for a file stored in a {@link NativeFileSystemStore}.
 * </p>
 */
@InterfaceAudience.Private
class FileMetadata {
  private final String key;
  private final long length;
  private final long lastModified;
  private final boolean isDir;
  private final PermissionStatus permissionStatus;
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
   */
  public FileMetadata(String key, long length, long lastModified,
      PermissionStatus permissionStatus) {
    this.key = key;
    this.length = length;
    this.lastModified = lastModified;
    this.isDir = false;
    this.permissionStatus = permissionStatus;
    this.blobMaterialization = BlobMaterialization.Explicit; // File are never
                                                             // implicit.
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
   */
  public FileMetadata(String key, long lastModified,
      PermissionStatus permissionStatus, BlobMaterialization blobMaterialization) {
    this.key = key;
    this.isDir = true;
    this.length = 0;
    this.lastModified = lastModified;
    this.permissionStatus = permissionStatus;
    this.blobMaterialization = blobMaterialization;
  }

  public boolean isDir() {
    return isDir;
  }

  public String getKey() {
    return key;
  }

  public long getLength() {
    return length;
  }

  public long getLastModified() {
    return lastModified;
  }

  public PermissionStatus getPermissionStatus() {
    return permissionStatus;
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

  @Override
  public String toString() {
    return "FileMetadata[" + key + ", " + length + ", " + lastModified + ", "
        + permissionStatus + "]";
  }
}
