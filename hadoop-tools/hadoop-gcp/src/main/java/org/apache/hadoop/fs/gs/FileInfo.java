/*
 * Copyright 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.gs;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Contains information about a file or a directory.
 *
 * <p>Note: This class wraps GoogleCloudStorageItemInfo, adds file system specific information and
 * hides bucket/object specific information.
 */
class FileInfo {

  // Info about the root path.
  public static final FileInfo ROOT_INFO =
      new FileInfo(GoogleCloudStorageFileSystem.GCS_ROOT, GoogleCloudStorageItemInfo.ROOT_INFO);

  // Path of this file or directory.
  private final URI path;

  // Information about the underlying GCS item.
  private final GoogleCloudStorageItemInfo itemInfo;

  /**
   * Constructs an instance of FileInfo.
   *
   * @param itemInfo Information about the underlying item.
   */
  private FileInfo(URI path, GoogleCloudStorageItemInfo itemInfo) {
    this.itemInfo = itemInfo;

    // Construct the path once.
    this.path = path;
  }

  /**
   * Gets the path of this file or directory.
   */
  public URI getPath() {
    return path;
  }

  /**
   * Indicates whether this item is a directory.
   */
  public boolean isDirectory() {
    return itemInfo.isDirectory();
  }

  /**
   * Indicates whether this item is an inferred directory.
   */
  public boolean isInferredDirectory() {
    return itemInfo.isInferredDirectory();
  }

  /**
   * Indicates whether this instance has information about the unique, shared root of the underlying
   * storage system.
   */
  public boolean isGlobalRoot() {
    return itemInfo.isGlobalRoot();
  }

  /**
   * Gets creation time of this item.
   *
   * <p>Time is expressed as milliseconds since January 1, 1970 UTC.
   */
  public long getCreationTime() {
    return itemInfo.getCreationTime();
  }

  /**
   * Gets the size of this file or directory.
   *
   * <p>For files, size is in number of bytes. For directories size is 0. For items that do not
   * exist, size is -1.
   */
  public long getSize() {
    return itemInfo.getSize();
  }

  /**
   * Gets the modification time of this file if one is set, otherwise the value of {@link
   * #getCreationTime()} is returned.
   *
   * <p>Time is expressed as milliseconds since January 1, 1970 UTC.
   */
  public long getModificationTime() {
    return itemInfo.getModificationTime();
  }

  /**
   * Retrieve file attributes for this file.
   *
   * @return A map of file attributes
   */
  public Map<String, byte[]> getAttributes() {
    return itemInfo.getMetadata();
  }

  /**
   * Indicates whether this file or directory exists.
   */
  public boolean exists() {
    return itemInfo.exists();
  }

  /**
   * Returns CRC32C checksum of the file or {@code null}.
   */
  public byte[] getCrc32cChecksum() {
    VerificationAttributes verificationAttributes = itemInfo.getVerificationAttributes();
    return verificationAttributes == null ? null : verificationAttributes.getCrc32c();
  }

  /**
   * Returns MD5 checksum of the file or {@code null}.
   */
  public byte[] getMd5Checksum() {
    VerificationAttributes verificationAttributes = itemInfo.getVerificationAttributes();
    return verificationAttributes == null ? null : verificationAttributes.getMd5hash();
  }

  /**
   * Gets information about the underlying item.
   */
  GoogleCloudStorageItemInfo getItemInfo() {
    return itemInfo;
  }

  /**
   * Gets string representation of this instance.
   */
  @Override
  public String toString() {
    return getPath() + (exists() ?
        ": created on: " + Instant.ofEpochMilli(getCreationTime()) :
        ": exists: no");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileInfo)) {
      return false;
    }
    FileInfo fileInfo = (FileInfo) o;
    return Objects.equals(path, fileInfo.path) && Objects.equals(itemInfo, fileInfo.itemInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, itemInfo);
  }

  /**
   * Handy factory method for constructing a FileInfo from a GoogleCloudStorageItemInfo while
   * potentially returning a singleton instead of really constructing an object for cases like ROOT.
   */
  public static FileInfo fromItemInfo(GoogleCloudStorageItemInfo itemInfo) {
    if (itemInfo.isRoot()) {
      return ROOT_INFO;
    }
    URI path = UriPaths.fromResourceId(itemInfo.getResourceId(), /* allowEmptyObjectName= */ true);
    return new FileInfo(path, itemInfo);
  }

  /**
   * Handy factory method for constructing a list of FileInfo from a list of
   * GoogleCloudStorageItemInfo.
   */
  public static List<FileInfo> fromItemInfos(List<GoogleCloudStorageItemInfo> itemInfos) {
    List<FileInfo> fileInfos = new ArrayList<>(itemInfos.size());
    for (GoogleCloudStorageItemInfo itemInfo : itemInfos) {
      fileInfos.add(fromItemInfo(itemInfo));
    }
    return fileInfos;
  }
}
