/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.snative;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * A subclass of {@link FileStatus} that contains the
 * Swift-specific rules of when a file is considered to be a directory.
 */
public class SwiftFileStatus extends FileStatus {

  public SwiftFileStatus() {
  }

  public SwiftFileStatus(long length,
                         boolean isdir,
                         int block_replication,
                         long blocksize, long modification_time, Path path) {
    super(length, isdir, block_replication, blocksize, modification_time, path);
  }

  public SwiftFileStatus(long length,
                         boolean isdir,
                         int block_replication,
                         long blocksize,
                         long modification_time,
                         long access_time,
                         FsPermission permission,
                         String owner, String group, Path path) {
    super(length, isdir, block_replication, blocksize, modification_time,
            access_time, permission, owner, group, path);
  }

  //HDFS2+ only

  public SwiftFileStatus(long length,
                         boolean isdir,
                         int block_replication,
                         long blocksize,
                         long modification_time,
                         long access_time,
                         FsPermission permission,
                         String owner, String group, Path symlink, Path path) {
    super(length, isdir, block_replication, blocksize, modification_time,
          access_time, permission, owner, group, symlink, path);
  }

  /**
   * Declare that the path represents a directory, which in the
   * SwiftNativeFileSystem means "is a directory or a 0 byte file"
   *
   * @return true if the status is considered to be a file
   */
  @Override
  public boolean isDirectory() {
    return super.isDirectory() || getLen() == 0;
  }

  /**
   * A entry is a file if it is not a directory.
   * By implementing it <i>and not marking as an override</i> this
   * subclass builds and runs in both Hadoop versions.
   * @return the opposite value to {@link #isDirectory()}
   */
  @Override
  public boolean isFile() {
    return !this.isDirectory();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append("{ ");
    sb.append("path=").append(getPath());
    sb.append("; isDirectory=").append(isDirectory());
    sb.append("; length=").append(getLen());
    sb.append("; blocksize=").append(getBlockSize());
    sb.append("; modification_time=").append(getModificationTime());
    sb.append("}");
    return sb.toString();
  }
}
