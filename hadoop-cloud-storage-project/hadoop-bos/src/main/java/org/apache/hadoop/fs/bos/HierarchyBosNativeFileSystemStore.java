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

import com.baidubce.services.bos.model.DeleteDirectoryResponse;
import com.baidubce.services.bos.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Store implementation for hierarchy (namespace-enabled)
 * BOS buckets. Supports native directory operations such as
 * recursive delete and atomic rename.
 */
public class HierarchyBosNativeFileSystemStore
    extends BosNativeFileSystemStore {

  /**
   * Constructs a store for a hierarchy bucket.
   *
   * @param bosClientProxy the BOS client proxy
   * @param bucketName     the name of the bucket
   * @param conf           the Hadoop configuration
   */
  public HierarchyBosNativeFileSystemStore(
      BosClientProxy bosClientProxy,
      String bucketName, Configuration conf) {
    super(bosClientProxy, bucketName, conf);
  }

  /**
   * {@inheritDoc}
   *
   * Checks whether the key is a directory by examining the
   * object type in the metadata.
   */
  @Override
  public boolean isDirectory(String key)
      throws IOException {
    try {
      ObjectMetadata meta =
          bosClientProxy.getObjectMetadata(
              bucketName, key);
      return "Directory".equals(meta.getObjectType());
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   *
   * Retrieves file metadata. For hierarchy buckets, the
   * object type metadata determines whether the key is a
   * directory.
   */
  @Override
  public FileMetadata retrieveMetadata(String key)
      throws IOException {
    boolean isFolder = false;

    ObjectMetadata meta =
        bosClientProxy.getObjectMetadata(
            bucketName, key);
    if (BaiduBosConstants.BOS_FILE_TYPE_DIRECTORY
        .equals(meta.getObjectType())) {
      isFolder = true;
    }
    long lastMod =
        meta.getLastModified() != null
            ? meta.getLastModified().getTime() : 0L;
    return new FileMetadata(
        key, meta.getContentLength(),
        lastMod, isFolder, meta);
  }

  /** {@inheritDoc} */
  @Override
  protected boolean isHierarchy() {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * Deletes a directory. For non-recursive deletes, verifies
   * the directory is empty first.
   */
  @Override
  public void deleteDirs(
      String key, boolean recursive) throws IOException {
    if (!recursive) {
      PartialListing listing =
          list(key, 1, null, "/");
      if (listing.getFiles().length
          + listing.getDirectories().length > 0) {
        throw new IOException(
            "Directory " + key + " is not empty.");
      }
      delete(key);
    } else {
      String marker = null;
      DeleteDirectoryResponse response = null;
      do {
        response = bosClientProxy.deleteDirectory(
            bucketName, key, true, marker);
        if (response != null
            && response.isTruncated()) {
          marker = response.getNextDeleteMarker();
        } else {
          break;
        }
      } while (response.isTruncated());
    }
  }

  /**
   * {@inheritDoc}
   *
   * Renames an object using the BOS rename API.
   */
  @Override
  public void rename(
      String srcKey, String dstKey,
      final boolean isFile) throws IOException {
    log.debug(
        "Renaming srcKey: {} to dstKey: {}"
            + " in bucket: {}",
        srcKey, dstKey, bucketName);
    try {
      bosClientProxy.renameObject(
          bucketName, srcKey, dstKey);
    } catch (FileNotFoundException e) {
      log.debug(
          "Renaming srcKey: {} to dstKey: {}"
              + " in bucket: {} failed,"
              + " FileNotFoundException",
          srcKey, dstKey, bucketName);
      throw e;
    }
  }
}
