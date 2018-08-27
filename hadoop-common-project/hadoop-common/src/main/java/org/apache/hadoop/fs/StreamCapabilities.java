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

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface to query streams for supported capabilities.
 *
 * Capability strings must be in lower case.
 *
 * Constant strings are chosen over enums in order to allow other file systems
 * to define their own capabilities.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface StreamCapabilities {
  /**
   * Stream hflush capability implemented by {@link Syncable#hflush()}.
   */
  String HFLUSH = "hflush";

  /**
   * Stream hsync capability implemented by {@link Syncable#hsync()}.
   */
  String HSYNC = "hsync";

  /**
   * Stream setReadahead capability implemented by
   * {@link CanSetReadahead#setReadahead(Long)}.
   */
  String READAHEAD = "in:readahead";

  /**
   * Stream setDropBehind capability implemented by
   * {@link CanSetDropBehind#setDropBehind(Boolean)}.
   */
  String DROPBEHIND = "dropbehind";

  /**
   * Stream unbuffer capability implemented by {@link CanUnbuffer#unbuffer()}.
   */
  String UNBUFFER = "in:unbuffer";

  /**
   * Does the Filesystem support get/set ACLs and related methods?
   */
  String FS_ACLS = "fs:acls";

  /**
   * Does the Filesystem support append operations?
   */
  String FS_APPEND = "fs:append";

  /**
   * Does the FS support {@code concat()}?
   */
  String FS_CONCAT = "fs:concat";

  /**
   * Does the FS support {@code setPermission(Path, FsPermission)}
   * and related methods?
   */
  String FS_PERMISSIONS = "fs:permissions";

  /**
   * Does the FS support path handles??
   */
  String FS_PATHHANDLES = "fs:pathhandles";

  /**
   * Does the FS support snapshot?
   */
  String FS_SNAPSHOTS = "fs:snapshots";

  /**
   * Does the FS support storage policies?
   */
  String FS_STORAGEPOLICY = "fs:storagepolicy";

  /**
   * Does the FS support symlinks?
   */
  String FS_SYMLINKS = "fs:symlinks";

  /**
   * Does the FS support truncate()?
   */
  String FS_TRUNCATE = "fs:truncate";

  /**
   * Does the Filesystem support XAttributes
   * {@code setXAttr(Path, String, byte[])} and related methods?
   */
  String FS_XATTRS = "fs:xattrs";

  /**
   * Capabilities that a stream can support and be queried for.
   */
  @Deprecated
  enum StreamCapability {
    HFLUSH(StreamCapabilities.HFLUSH),
    HSYNC(StreamCapabilities.HSYNC);

    private final String capability;

    StreamCapability(String value) {
      this.capability = value;
    }

    public final String getValue() {
      return capability;
    }
  }

  /**
   * Query the stream for a specific capability.
   *
   * @param capability string to query the stream support for.
   * @return True if the stream supports capability.
   */
  boolean hasCapability(String capability);
}

