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

  private static final long serialVersionUID = -5955674081978903922L;

  private Tristate isEmptyDirectory;
  private String eTag;
  private String versionId;

  /**
   * Create a directory status.
   * @param isemptydir is this an empty directory?
   * @param path the path
   * @param owner the owner
   */
  public S3AFileStatus(boolean isemptydir,
      Path path,
      String owner) {
    this(Tristate.fromBool(isemptydir), path, owner);
  }

  /**
   * Create a directory status.
   * @param isemptydir is this an empty directory?
   * @param path the path
   * @param owner the owner
   */
  public S3AFileStatus(Tristate isemptydir,
      Path path,
      String owner) {
    this(path,
        true,
        isemptydir,
        0,
        0,
        0,
        owner,
        null,
        null
    );
  }

  /**
   * A simple file.
   * @param length file length
   * @param modification_time mod time
   * @param path path
   * @param blockSize block size
   * @param owner owner
   * @param eTag eTag of the S3 object if available, else null
   * @param versionId versionId of the S3 object if available, else null
   */
  public S3AFileStatus(long length, long modification_time, Path path,
      long blockSize, String owner, String eTag, String versionId) {
    this(path,
        false,
        Tristate.FALSE,
        length,
        modification_time,
        blockSize,
        owner,
        eTag,
        versionId
    );
  }

  /**
   * Either a file or directory.
   * @param path path
   * @param isDir is this a directory?
   * @param isemptydir is this an empty directory?
   * @param length file length
   * @param modificationTime mod time
   * @param blockSize block size
   * @param owner owner
   * @param eTag eTag of the S3 object if available, else null
   * @param versionId versionId of the S3 object if available, else null
   */
  S3AFileStatus(Path path,
      boolean isDir,
      Tristate isemptydir,
      long length,
      long modificationTime,
      long blockSize,
      String owner,
      String eTag,
      String versionId) {
    super(length, isDir, 1, blockSize, modificationTime,
        0, null, owner, owner, null,
        path, false, true, false);
    this.isEmptyDirectory = isemptydir;
    this.eTag = eTag;
    this.versionId = versionId;
  }

  /**
   * Convenience constructor for creating from a vanilla FileStatus plus
   * an isEmptyDirectory flag.
   * @param source FileStatus to convert to S3AFileStatus
   * @param isEmptyDirectory TRUE/FALSE if known to be / not be an empty
   *     directory, UNKNOWN if that information was not computed.
   * @param eTag eTag of the S3 object if available, else null
   * @param versionId versionId of the S3 object if available, else null
   * @return a new S3AFileStatus
   */
  public static S3AFileStatus fromFileStatus(FileStatus source,
      Tristate isEmptyDirectory, String eTag, String versionId) {
    if (source.isDirectory()) {
      return new S3AFileStatus(isEmptyDirectory, source.getPath(),
          source.getOwner());
    } else {
      return new S3AFileStatus(source.getLen(), source.getModificationTime(),
          source.getPath(), source.getBlockSize(), source.getOwner(),
          eTag, versionId);
    }
  }


  /**
   * @return FALSE if status is not a directory, or its a dir, but known to
   * not be empty.  TRUE if it is an empty directory.  UNKNOWN if it is a
   * directory, but we have not computed whether or not it is empty.
   */
  public Tristate isEmptyDirectory() {
    return isEmptyDirectory;
  }

  /**
   * Update isEmptyDirectory attribute.
   * @param isEmptyDirectory new isEmptyDirectory value
   */
  public void setIsEmptyDirectory(Tristate isEmptyDirectory) {
    this.isEmptyDirectory = isEmptyDirectory;
  }

  /**
   * @return the S3 object eTag when available, else null.
   */
  public String getETag() {
    return eTag;
  }

  /**
   * @return the S3 object versionId when available, else null.
   */
  public String getVersionId() {
    return versionId;
  }

  /**
   * set the S3 object versionId, else null.
   * @param versionId version ID or null.
   */
  public void setVersionId(final String versionId) {
    this.versionId = versionId;
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
    return super.toString()
        + String.format(" isEmptyDirectory=%s", isEmptyDirectory().name()
        + String.format(" eTag=%s", eTag)
        + String.format(" versionId=%s", versionId));
  }

}
