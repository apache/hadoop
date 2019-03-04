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

import java.util.List;

import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Common path capabilities.
 */
public final class CommonPathCapabilities {

  public CommonPathCapabilities() {
  }

  /**
   * Does the Filesystem support
   * {@link FileSystem#setAcl(Path, List)},
   * {@link FileSystem#getAclStatus(Path)}
   * and related methods?
   * Value: {@value}.
   */
  public static final String FS_ACLS = "fs.paths.acls";

  /**
   * Does the Filesystem support {@link FileSystem#append(Path)}?
   * Value: {@value}.
   */
  public static final String FS_APPEND = "fs.paths.append";

  /**
   * Does the FS support {@link FileSystem#getFileChecksum(Path)}?
   * Value: {@value}.
   */
  public static final String FS_CHECKSUMS = "fs.paths.checksums";

  /**
   * Does the FS support {@link FileSystem#concat(Path, Path[])}?
   * Value: {@value}.
   */
  public static final String FS_CONCAT = "fs.paths.concat";

  /**
   * Does the filesystem support Delegation Tokens?
   * Value: {@value}.
   */
  public static final String FS_DELEGATION_TOKENS =
      "fs.paths.delegation.tokens";

  /**
   * Does the FS support {@link FileSystem#listCorruptFileBlocks(Path)} ()}?
   * Value: {@value}.
   */
  public static final String FS_LIST_CORRUPT_FILE_BLOCKS =
      "fs.paths.list-corrupt-file-blocks";

  /**
   * Does the FS support
   * {@link FileSystem#createPathHandle(FileStatus, Options.HandleOpt...)}
   * and related methods?
   * Value: {@value}.
   */
  public static final String FS_PATHHANDLES = "fs.paths.pathhandles";

  /**
   * Does the FS support {@link FileSystem#setPermission(Path, FsPermission)}
   * and related methods?
   * Value: {@value}.
   */
  public static final String FS_PERMISSIONS = "fs.paths.permissions";

  /**
   * Does this filesystem connector only support filesystem read operations?
   * For example, the {@code HttpFileSystem} is always read-only.
   * This is different from "is the specific instance and path read only?",
   * which must be determined by checking permissions (where supported), or
   * attempting write operations under a path.
   * Value: {@value}.
   */
  public static final String FS_READ_ONLY_CONNECTOR =
      "fs.paths.read-only-connector";

  /**
   * Does the FS support snapshots through
   * {@link FileSystem#createSnapshot(Path)} and related methods??
   * Value: {@value}.
   */
  public static final String FS_SNAPSHOTS = "fs.paths.snapshots";

  /**
   * Does the FS support {@link FileSystem#setStoragePolicy(Path, String)}
   * and related methods?
   * Value: {@value}.
   */
  public static final String FS_STORAGEPOLICY =
      "fs.paths.storagepolicy";

  /**
   * Does the FS support symlinks through
   * {@link FileSystem#createSymlink(Path, Path, boolean)} and related methods?
   * Value: {@value}.
   */
  public static final String FS_SYMLINKS =
      "fs.paths.symlinks";

  /**
   * Does the FS support {@link FileSystem#truncate(Path, long)} ?
   * Value: {@value}.
   */
  public static final String FS_TRUNCATE =
      "fs.paths.truncate";

  /**
   * Does the Filesystem support XAttributes through
   * {@link FileSystem#setXAttr(Path, String, byte[])} and related methods?
   * Value: {@value}.
   */
  public static final String FS_XATTRS = "fs.paths.xattrs";

}
