/*
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

package org.apache.hadoop.fs.bos;

import com.baidubce.services.bos.model.ObjectMetadata;
import org.apache.hadoop.fs.FileStatus;

import java.util.Set;

/**
 * Holds basic metadata for a file stored in a
 * {@link BosNativeFileSystemStore}.
 */
public class FileMetadata {

  private final String key;
  private final long length;
  private final long lastModified;
  private final boolean isFolder;
  private final String userName;
  private final String groupName;
  private final String permission;
  private final Set<FileStatus.AttrFlags> attr;
  private ObjectMetadata objectMetadata = null;

  /**
   * Constructs a FileMetadata with minimal fields.
   *
   * @param key          the object key
   * @param length       the content length in bytes
   * @param lastModified the last modified timestamp
   * @param isFolder     whether this entry is a folder
   */
  public FileMetadata(
      String key, long length,
      long lastModified, boolean isFolder) {
    this(key, length, lastModified, isFolder,
        null, null, null, false);
  }

  /**
   * Constructs a FileMetadata with user, group, and
   * permission fields.
   *
   * @param key          the object key
   * @param length       the content length in bytes
   * @param lastModified the last modified timestamp
   * @param isFolder     whether this entry is a folder
   * @param userName     the owner user name
   * @param groupName    the owner group name
   * @param permission   the permission string
   */
  public FileMetadata(
      String key, long length,
      long lastModified, boolean isFolder,
      String userName, String groupName,
      String permission) {
    this(key, length, lastModified, isFolder,
        userName, groupName, permission, false);
  }

  /**
   * Constructs a FileMetadata from BOS object metadata.
   *
   * @param key            the object key
   * @param length         the content length in bytes
   * @param lastModified   the last modified timestamp
   * @param isFolder       whether this entry is a folder
   * @param objectMetadata the BOS object metadata
   */
  public FileMetadata(
      String key, long length,
      long lastModified, boolean isFolder,
      ObjectMetadata objectMetadata) {
    this(key, length, lastModified, isFolder,
        objectMetadata.getUserMetaDataOf(
            BaiduBosConstants.BOS_FILE_PATH_USER_KEY),
        objectMetadata.getUserMetaDataOf(
            BaiduBosConstants.BOS_FILE_PATH_GROUP_KEY),
        null,
        false);
    this.objectMetadata = objectMetadata;
  }

  /**
   * Constructs a FileMetadata with all fields.
   *
   * @param key          the object key
   * @param length       the content length in bytes
   * @param lastModified the last modified timestamp
   * @param isFolder     whether this entry is a folder
   * @param userName     the owner user name
   * @param groupName    the owner group name
   * @param permission   the permission string
   * @param hasAcl       whether the object has an ACL
   */
  public FileMetadata(
      String key, long length,
      long lastModified, boolean isFolder,
      String userName, String groupName,
      String permission, boolean hasAcl) {
    this.key = key;
    this.length = length;
    this.lastModified = lastModified;
    this.isFolder = isFolder;
    this.userName = userName;
    this.groupName = groupName;
    this.permission = permission;
    this.attr = FileStatus.attributes(
        hasAcl, false, false, false);
  }

  /**
   * Returns the object key.
   *
   * @return the key
   */
  public String getKey() {
    return key;
  }

  /**
   * Returns the content length in bytes.
   *
   * @return the length
   */
  public long getLength() {
    return length;
  }

  /**
   * Returns the last modified timestamp.
   *
   * @return the last modified time in milliseconds
   */
  public long getLastModified() {
    return lastModified;
  }

  /**
   * Returns whether this entry is a folder.
   *
   * @return true if this is a folder
   */
  public boolean isFolder() {
    return isFolder;
  }

  /**
   * Returns the owner user name.
   *
   * @return the user name
   */
  public String getUserName() {
    return this.userName;
  }

  /**
   * Returns the owner group name.
   *
   * @return the group name
   */
  public String getGroupName() {
    return this.groupName;
  }

  /**
   * Returns the permission string.
   *
   * @return the permission
   */
  public String getPermission() {
    return this.permission;
  }

  /**
   * Returns the attribute flags.
   *
   * @return the set of attribute flags
   */
  public Set<FileStatus.AttrFlags> getAttr() {
    return attr;
  }

  /**
   * Returns the BOS object metadata, if available.
   *
   * @return the object metadata, or null
   */
  public ObjectMetadata getObjectMetadata() {
    return objectMetadata;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "FileMetadata[" + key + ", " + length
        + ", " + lastModified + ", " + isFolder + "]";
  }

}
