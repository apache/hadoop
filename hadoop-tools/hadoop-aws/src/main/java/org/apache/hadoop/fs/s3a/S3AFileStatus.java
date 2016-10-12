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
package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * File status for an S3A "file".
 * Modification time is trouble, see {@link #getModificationTime()}.
 *
 * The subclass is private as it should not be created directly.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3AFileStatus extends FileStatus {
  private boolean isEmptyDirectory;

  /**
   * Create a directory status.
   * @param isemptydir is this an empty directory?
   * @param path the path
   * @param owner the owner
   */
  public S3AFileStatus(boolean isemptydir,
      Path path,
      String owner) {
    super(0, true, 1, 0, 0, path);
    isEmptyDirectory = isemptydir;
    setOwner(owner);
    setGroup(owner);
  }

  /**
   * A simple file.
   * @param length file length
   * @param modification_time mod time
   * @param path path
   * @param blockSize block size
   * @param owner owner
   */
  public S3AFileStatus(long length, long modification_time, Path path,
      long blockSize, String owner) {
    super(length, false, 1, blockSize, modification_time, path);
    isEmptyDirectory = false;
    setOwner(owner);
    setGroup(owner);
  }

  public boolean isEmptyDirectory() {
    return isEmptyDirectory;
  }

  /** Compare if this object is equal to another object.
   * @param   o the object to be compared.
   * @return  true if two file status has the same path name; false if not.
   */
  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }
  
  /**
   * Returns a hash code value for the object, which is defined as
   * the hash code of the path name.
   *
   * @return  a hash code value for the path name.
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /** Get the modification time of the file/directory.
   *
   * s3a uses objects as "fake" directories, which are not updated to
   * reflect the accurate modification time. We choose to report the
   * current time because some parts of the ecosystem (e.g. the
   * HistoryServer) use modification time to ignore "old" directories.
   *
   * @return for files the modification time in milliseconds since January 1,
   *         1970 UTC or for directories the current time.
   */
  @Override
  public long getModificationTime(){
    if(isDirectory()){
      return System.currentTimeMillis();
    } else {
      return super.getModificationTime();
    }
  }

  @Override
  public String toString() {
    return super.toString() +
        String.format(" isEmptyDirectory=%s", isEmptyDirectory());
  }

}
