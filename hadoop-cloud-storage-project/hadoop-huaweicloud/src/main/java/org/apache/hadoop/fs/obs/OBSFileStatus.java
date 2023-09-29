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

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * File status for an OBS file.
 *
 * <p>The subclass is private as it should not be created directly.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class OBSFileStatus extends FileStatus {
  /**
   * Create a directory status.
   *
   * @param path  the path
   * @param owner the owner
   */
  OBSFileStatus(final Path path, final String owner) {
    super(0, true, 1, 0, 0, path);
    setOwner(owner);
    setGroup(owner);
  }

  /**
   * Create a directory status.
   *
   * @param modificationTime modification time
   * @param path             the path
   * @param owner            the owner
   */
  OBSFileStatus(final Path path, final long modificationTime,
      final String owner) {
    super(0, true, 1, 0, modificationTime, path);
    setOwner(owner);
    setGroup(owner);
  }

  /**
   * Create a directory status.
   *
   * @param modificationTime modification time
   * @param accessTime       access time
   * @param path             the path
   * @param owner            the owner
   */
  OBSFileStatus(final Path path, final long modificationTime,
      final long accessTime,
      final String owner) {
    super(0, true, 1, 0, modificationTime, accessTime, null, owner, owner,
        path);
  }

  /**
   * A simple file.
   *
   * @param length           file length
   * @param modificationTime mod time
   * @param path             path
   * @param blockSize        block size
   * @param owner            owner
   */
  OBSFileStatus(
      final long length, final long modificationTime, final Path path,
      final long blockSize,
      final String owner) {
    super(length, false, 1, blockSize, modificationTime, path);
    setOwner(owner);
    setGroup(owner);
  }
}
