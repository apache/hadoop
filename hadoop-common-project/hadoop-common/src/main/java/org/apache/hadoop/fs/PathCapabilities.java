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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.permission.FsPermission;

/**
 * The Path counterpoint to {@link StreamCapabilities}; a query to see if,
 * a FileSystem/FileContext instance has a specific capability under the given
 * path.
 */
public interface PathCapabilities {

  /**
   * Does the Filesystem support
   * {@link FileSystem#setAcl(Path, List)},
   * {@link FileSystem#getAclStatus(Path)} 
   * and related methods?
   * Value: {@value}.
   */
  String FS_ACLS = "fs:acls";

  /**
   * Does the Filesystem support {@link FileSystem#append(Path)}?
   * Value: {@value}.
   */
  String FS_APPEND = "fs:append";

  /**
   * Does the FS support {@link FileSystem#getFileChecksum(Path)}?
   * Value: {@value}.
   */
  String FS_CHECKSUMS = "fs:checksums";

  /**
   * Does the FS support {@link FileSystem#concat(Path, Path[])}?
   * Value: {@value}.
   */
  String FS_CONCAT = "fs:concat";

  /**
   * Does the FS support {@link FileSystem#listCorruptFileBlocks(Path)} ()}?
   * Value: {@value}.
   */
  String FS_LIST_CORRUPT_FILE_BLOCKS = "fs:list-corrupt-file-blocks";

  /**
   * Does the FS support {@link FileSystem#setPermission(Path, FsPermission)}
   * and related methods?
   * Value: {@value}.
   */
  String FS_PERMISSIONS = "fs:permissions";

  /**
   * Does the FS support
   * {@link FileSystem#createPathHandle(FileStatus, Options.HandleOpt...)}
   * and related methods?
   * Value: {@value}.
   */
  String FS_PATHHANDLES = "fs:pathhandles";

  /**
   * Does this filesystem connector only support filesystem read operations?
   * For example, the {@code HttpFileSystem} is always read-only.
   * This is different from "is the specific instance and path read only?", which
   * must be determined by checking permissions (where supported), or
   * attempting write operations under a path.
   * Value: {@value}.
   */
  String FS_READ_ONLY_CONNECTOR = "fs:read-only-connector";

  /**
   * Does the FS support snapshots through
   * {@link FileSystem#createSnapshot(Path)} and related methods??
   * Value: {@value}.
   */
  String FS_SNAPSHOTS = "fs:snapshots";

  /**
   * Does the FS support {@link FileSystem#setStoragePolicy(Path, String)} 
   * and related methods?
   * Value: {@value}.
   */
  String FS_STORAGEPOLICY = "fs:storagepolicy";

  /**
   * Does the FS support symlinks through
   * {@link FileSystem#createSymlink(Path, Path, boolean)} and related methods?
   * Value: {@value}.
   */
  String FS_SYMLINKS = "fs:symlinks";

  /**
   * Does the FS support {@link FileSystem#truncate(Path, long)} ?
   * Value: {@value}.
   */
  String FS_TRUNCATE = "fs:truncate";

  /**
   * Does the Filesystem support XAttributes through
   * {@link FileSystem#setXAttr(Path, String, byte[])} and related methods?
   * Value: {@value}.
   */
  String FS_XATTRS = "fs:xattrs";

  /**
   * Probe for a filesystem instance offering a specific capability under the
   * given path.
   * If the function returns {@code true}, the filesystem is explicitly
   * declaring that the capability is available.
   * If the function returns {@code false}, it can mean one of:
   * <ul>
   *   <li>The capability is not known.</li>
   *   <li>The capability is known but it is not supported.</li>
   *   <li>The capability is known but the filesystem does not know if it
   *   is supported under the supplied path it.</li>
   * </ul>
   * The core guarantee which a caller can rely on is: if the predicate
   * returns true, then the specific operation/behavior can be expected to be
   * supported.
   * @param path path to query the capability of.
   * @param capability non-null, non-empty string to query the path for support.
   * @return true if the capability is supported under that part of the FS.
   * @throws IOException this should not be raised, except on problems
   * resolving paths or relaying the call.
   * @throws IllegalArgumentException invalid arguments
   */
  boolean hasPathCapability(Path path, String capability)
      throws IOException;
}
