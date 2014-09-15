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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class S3AFileStatus extends FileStatus {
  private boolean isEmptyDirectory;

  // Directories
  public S3AFileStatus(boolean isdir, boolean isemptydir, Path path) {
    super(0, isdir, 1, 0, 0, path);
    isEmptyDirectory = isemptydir;
  }

  // Files
  public S3AFileStatus(long length, long modification_time, Path path) {
    super(length, false, 1, 0, modification_time, path);
    isEmptyDirectory = false;
  }

  public boolean isEmptyDirectory() {
    return isEmptyDirectory;
  }
  
  /** Compare if this object is equal to another object
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
}
