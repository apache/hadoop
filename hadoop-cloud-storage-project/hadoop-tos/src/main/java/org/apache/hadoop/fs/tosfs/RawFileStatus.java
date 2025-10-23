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

package org.apache.hadoop.fs.tosfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.Objects;

public class RawFileStatus extends FileStatus {
  private final byte[] checksum;

  /**
   * File status of directory.
   *
   * @param length           the length of the file, 0 if it is a directory.
   * @param isdir            whether it is a directory.
   * @param blocksize        the size of the block.
   * @param modificationTime the last modified time.
   * @param path             the file status path.
   * @param owner            the owner.
   * @param checksum         the checksum of the file.
   */
  public RawFileStatus(
      long length, boolean isdir, long blocksize,
      long modificationTime, Path path, String owner, byte[] checksum) {
    super(length, isdir, 1, blocksize, modificationTime, path);
    setOwner(owner);
    setGroup(owner);
    this.checksum = checksum;
  }

  public byte[] checksum() {
    return checksum;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), Arrays.hashCode(checksum));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RawFileStatus)) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!super.equals(o)) {
      return false;
    }

    RawFileStatus other = (RawFileStatus)o;
    return Arrays.equals(checksum, other.checksum);
  }
}
