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

package org.apache.hadoop.fs.s3a.s3guard;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.Tristate;

/**
 * {@code PathMetadata} models path metadata stored in the
 * {@link MetadataStore}. The lastUpdated field is implicitly set to 0 in the
 * constructors without that parameter to show that it will be initialized
 * with 0 if not set otherwise.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PathMetadata extends ExpirableMetadata {

  private S3AFileStatus fileStatus;
  private Tristate isEmptyDirectory;
  private boolean isDeleted;

  /**
   * Create a tombstone from the current time.
   * It is mandatory to set the lastUpdated field to update when the
   * tombstone state has changed to set when the entry got deleted.
   *
   * @param path path to tombstone
   * @param lastUpdated last updated time on which expiration is based.
   * @return the entry.
   */
  public static PathMetadata tombstone(Path path, long lastUpdated) {
    S3AFileStatus s3aStatus = new S3AFileStatus(0,
        System.currentTimeMillis(), path, 0, null,
        null, null);
    return new PathMetadata(s3aStatus, Tristate.UNKNOWN, true, lastUpdated);
  }

  /**
   * Creates a new {@code PathMetadata} containing given {@code FileStatus}.
   * lastUpdated field will be updated to 0 implicitly in this constructor.
   *
   * @param fileStatus file status containing an absolute path.
   */
  public PathMetadata(S3AFileStatus fileStatus) {
    this(fileStatus, Tristate.UNKNOWN, false, 0);
  }

  /**
   * Creates a new {@code PathMetadata} containing given {@code FileStatus}.
   *
   * @param fileStatus file status containing an absolute path.
   * @param lastUpdated last updated time on which expiration is based.
   */
  public PathMetadata(S3AFileStatus fileStatus, long lastUpdated) {
    this(fileStatus, Tristate.UNKNOWN, false, lastUpdated);
  }

  /**
   * Creates a new {@code PathMetadata}.
   * lastUpdated field will be updated to 0 implicitly in this constructor.
   *
   * @param fileStatus file status containing an absolute path.
   * @param isEmptyDir empty directory {@link Tristate}
   */
  public PathMetadata(S3AFileStatus fileStatus, Tristate isEmptyDir) {
    this(fileStatus, isEmptyDir, false, 0);
  }

  /**
   * Creates a new {@code PathMetadata}.
   * lastUpdated field will be updated to 0 implicitly in this constructor.
   *
   * @param fileStatus file status containing an absolute path.
   * @param isEmptyDir empty directory {@link Tristate}
   * @param isDeleted deleted / tombstoned flag
   */
  public PathMetadata(S3AFileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted) {
    this(fileStatus, isEmptyDir, isDeleted, 0);
  }

  /**
   * Creates a new {@code PathMetadata}.
   *
   * @param fileStatus file status containing an absolute path.
   * @param isEmptyDir empty directory {@link Tristate}
   * @param isDeleted deleted / tombstoned flag
   * @param lastUpdated last updated time on which expiration is based.
   */
  public PathMetadata(S3AFileStatus fileStatus, Tristate isEmptyDir, boolean
      isDeleted, long lastUpdated) {
    Preconditions.checkNotNull(fileStatus, "fileStatus must be non-null");
    Preconditions.checkNotNull(fileStatus.getPath(), "fileStatus path must be" +
        " non-null");
    Preconditions.checkArgument(fileStatus.getPath().isAbsolute(), "path must" +
        " be absolute");
    Preconditions.checkArgument(lastUpdated >=0, "lastUpdated parameter must "
        + "be greater or equal to 0.");
    this.fileStatus = fileStatus;
    this.isEmptyDirectory = isEmptyDir;
    this.isDeleted = isDeleted;
    this.setLastUpdated(lastUpdated);
  }

  /**
   * @return {@code FileStatus} contained in this {@code PathMetadata}.
   */
  public final S3AFileStatus getFileStatus() {
    return fileStatus;
  }

  /**
   * Query if a directory is empty.
   * @return Tristate.TRUE if this is known to be an empty directory,
   * Tristate.FALSE if known to not be empty, and Tristate.UNKNOWN if the
   * MetadataStore does have enough information to determine either way.
   */
  public Tristate isEmptyDirectory() {
    return isEmptyDirectory;
  }

  void setIsEmptyDirectory(Tristate isEmptyDirectory) {
    this.isEmptyDirectory = isEmptyDirectory;
    fileStatus.setIsEmptyDirectory(isEmptyDirectory);
  }

  public boolean isDeleted() {
    return isDeleted;
  }

  void setIsDeleted(boolean isDeleted) {
    this.isDeleted = isDeleted;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PathMetadata)) {
      return false;
    }
    return this.fileStatus.equals(((PathMetadata)o).fileStatus);
  }

  @Override
  public int hashCode() {
    return fileStatus.hashCode();
  }

  @Override
  public String toString() {
    return "PathMetadata{" +
        "fileStatus=" + fileStatus +
        "; isEmptyDirectory=" + isEmptyDirectory +
        "; isDeleted=" + isDeleted +
        "; lastUpdated=" + super.getLastUpdated() +
        '}';
  }

  /**
   * Log contents to supplied StringBuilder in a pretty fashion.
   * @param sb target StringBuilder
   */
  public void prettyPrint(StringBuilder sb) {
    sb.append(String.format("%-5s %-20s %-7d %-8s %-6s %-20s %-20s",
        fileStatus.isDirectory() ? "dir" : "file",
        fileStatus.getPath().toString(), fileStatus.getLen(),
        isEmptyDirectory.name(), isDeleted,
        fileStatus.getETag(), fileStatus.getVersionId()));
    sb.append(fileStatus);
  }

  public String prettyPrint() {
    StringBuilder sb = new StringBuilder();
    prettyPrint(sb);
    return sb.toString();
  }
}
