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

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Common path capabilities.
 */
public final class CommonPathCapabilities {

  private CommonPathCapabilities() {
  }

  /**
   * Does the store support
   * {@code FileSystem.setAcl(Path, List)},
   * {@code FileSystem.getAclStatus(Path)}
   * and related methods?
   * Value: {@value}.
   */
  public static final String FS_ACLS = "fs.capability.paths.acls";

  /**
   * Does the store support {@code FileSystem.append(Path)}?
   * Value: {@value}.
   */
  public static final String FS_APPEND = "fs.capability.paths.append";

  /**
   * Does the store support {@code FileSystem.getFileChecksum(Path)}?
   * Value: {@value}.
   */
  public static final String FS_CHECKSUMS = "fs.capability.paths.checksums";

  /**
   * Does the store support {@code FileSystem.concat(Path, Path[])}?
   * Value: {@value}.
   */
  public static final String FS_CONCAT = "fs.capability.paths.concat";

  /**
   * Does the store support {@code FileSystem.listCorruptFileBlocks(Path)} ()}?
   * Value: {@value}.
   */
  public static final String FS_LIST_CORRUPT_FILE_BLOCKS =
      "fs.capability.paths.list-corrupt-file-blocks";

  /**
   * Does the store support
   * {@code FileSystem.createPathHandle(FileStatus, Options.HandleOpt...)}
   * and related methods?
   * Value: {@value}.
   */
  public static final String FS_PATHHANDLES = "fs.capability.paths.pathhandles";

  /**
   * Does the store support {@code FileSystem.setPermission(Path, FsPermission)}
   * and related methods?
   * Value: {@value}.
   */
  public static final String FS_PERMISSIONS = "fs.capability.paths.permissions";

  /**
   * Does this filesystem connector only support filesystem read operations?
   * For example, the {@code HttpFileSystem} is always read-only.
   * This is different from "is the specific instance and path read only?",
   * which must be determined by checking permissions (where supported), or
   * attempting write operations under a path.
   * Value: {@value}.
   */
  public static final String FS_READ_ONLY_CONNECTOR =
      "fs.capability.paths.read-only-connector";

  /**
   * Does the store support snapshots through
   * {@code FileSystem.createSnapshot(Path)} and related methods??
   * Value: {@value}.
   */
  public static final String FS_SNAPSHOTS = "fs.capability.paths.snapshots";

  /**
   * Does the store support {@code FileSystem.setStoragePolicy(Path, String)}
   * and related methods?
   * Value: {@value}.
   */
  public static final String FS_STORAGEPOLICY =
      "fs.capability.paths.storagepolicy";

  /**
   * Does the store support symlinks through
   * {@code FileSystem.createSymlink(Path, Path, boolean)} and related methods?
   * Value: {@value}.
   */
  public static final String FS_SYMLINKS =
      "fs.capability.paths.symlinks";

  /**
   * Does the store support {@code FileSystem#truncate(Path, long)} ?
   * Value: {@value}.
   */
  public static final String FS_TRUNCATE =
      "fs.capability.paths.truncate";

  /**
   * Does the store support XAttributes through
   * {@code FileSystem#.setXAttr()} and related methods?
   * Value: {@value}.
   */
  public static final String FS_XATTRS = "fs.capability.paths.xattrs";

  /**
   * Probe for support for {@link BatchListingOperations}.
   */
  @InterfaceStability.Unstable
  public static final String FS_EXPERIMENTAL_BATCH_LISTING =
      "fs.capability.batch.listing";

  /**
   * Does the store support multipart uploading?
   * Value: {@value}.
   */
  public static final String FS_MULTIPART_UPLOADER =
      "fs.capability.multipart.uploader";


  /**
   * Stream abort() capability implemented by {@link Abortable#abort()}.
   * Value: {@value}.
   */
  public static final String ABORTABLE_STREAM =
      "fs.capability.outputstream.abortable";
}
