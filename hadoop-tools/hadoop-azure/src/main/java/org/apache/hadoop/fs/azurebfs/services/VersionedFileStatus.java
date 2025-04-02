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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.EtagSource;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * A File status with version info extracted from the etag value returned
 * in a LIST or HEAD request.
 * The etag is included in the java serialization.
 */
public class VersionedFileStatus extends FileStatus implements EtagSource {

  /**
   * The superclass is declared serializable; this subclass can also
   * be serialized.
   */
  private static final long serialVersionUID = -2009013240419749458L;

  /**
   * The etag of an object.
   * Not-final so that serialization via reflection will preserve the value.
   */
  private String version;

  private String encryptionContext;

  public VersionedFileStatus(
      final String owner, final String group, final FsPermission fsPermission, final boolean hasAcl,
      final long length, final boolean isdir, final int blockReplication,
      final long blocksize, final long modificationTime, final Path path,
      final String version, final String encryptionContext) {
    super(length, isdir, blockReplication, blocksize, modificationTime, 0,
        fsPermission,
        owner,
        group,
        null,
        path,
        hasAcl, false, false);

    this.version = version;
    this.encryptionContext = encryptionContext;
  }

  /** Compare if this object is equal to another object.
   * @param   obj the object to be compared.
   * @return  true if two file status has the same path name; false if not.
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FileStatus)) {
      return false;
    }

    FileStatus other = (FileStatus) obj;

    if (!this.getPath().equals(other.getPath())) {// compare the path
      return false;
    }

    if (other instanceof VersionedFileStatus) {
      return this.version.equals(((VersionedFileStatus) other).version);
    }

    return true;
  }

  /**
   * Returns a hash code value for the object, which is defined as
   * the hash code of the path name.
   *
   * @return  a hash code value for the path name and version
   */
  @Override
  public int hashCode() {
    int hash = getPath().hashCode();
    hash = 89 * hash + (this.version != null ? this.version.hashCode() : 0);
    return hash;
  }

  /**
   * Returns the version of this FileStatus
   *
   * @return  a string value for the FileStatus version
   */
  public String getVersion() {
    return this.version;
  }

  /**
   * Returns the etag of this FileStatus.
   * @return a string value for the FileStatus etag.
   */
  @Override
  public String getEtag() {
    return getVersion();
  }

  /**
   * Returns the encryption context of this FileStatus
   * @return a string value for the FileStatus encryption context
   */
  public String getEncryptionContext() {
    return encryptionContext;
  }

  /**
   * Returns a string representation of the object.
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "VersionedFileStatus{");
    sb.append(super.toString());
    sb.append("; version='").append(version).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
