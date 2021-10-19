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

package org.apache.hadoop.hdfs.client;

import java.util.Optional;

import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;

public final class DfsPathCapabilities {

  private DfsPathCapabilities() {
  }

  /**
   * Common implementation of {@code hasPathCapability} for DFS and webhdfs.
   * @param path path to check
   * @param capability capability
   * @return either a value to return or, if empty, a cue for the FS to
   * pass up to its superclass.
   */
  public static Optional<Boolean> hasPathCapability(final Path path,
      final String capability) {
    switch (validatePathCapabilityArgs(path, capability)) {

    case CommonPathCapabilities.FS_ACLS:
    case CommonPathCapabilities.FS_APPEND:
    case CommonPathCapabilities.FS_CHECKSUMS:
    case CommonPathCapabilities.FS_CONCAT:
    case CommonPathCapabilities.FS_LIST_CORRUPT_FILE_BLOCKS:
    case CommonPathCapabilities.FS_MULTIPART_UPLOADER:
    case CommonPathCapabilities.FS_PATHHANDLES:
    case CommonPathCapabilities.FS_PERMISSIONS:
    case CommonPathCapabilities.FS_SNAPSHOTS:
    case CommonPathCapabilities.FS_STORAGEPOLICY:
    case CommonPathCapabilities.FS_XATTRS:
      return Optional.of(true);
    case CommonPathCapabilities.FS_SYMLINKS:
      return Optional.of(FileSystem.areSymlinksEnabled());
    default:
      return Optional.empty();
    }
  }
}
