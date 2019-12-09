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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Tristate;

/**
 * {@code PathMetadata} models path metadata stored in the
 * {@link MetadataStore}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PathMetadata {

  private final FileStatus fileStatus;
  private Tristate isEmptyDirectory;
  private boolean isDeleted;

  /**
   * Create a tombstone from the current time.
   * @param path path to tombstone
   * @return the entry.
   */
  public static PathMetadata tombstone(Path path) {
    long now = System.currentTimeMillis();
    FileStatus status = new FileStatus(0, false, 0, 0, now, path);
    return new PathMetadata(status, Tristate.UNKNOWN, true);
  }

  /**
   * Creates a new {@code PathMetadata} containing given {@code FileStatus}.
   * @param fileStatus file status containing an absolute path.
   */
  public PathMetadata(FileStatus fileStatus) {
    this(fileStatus, Tristate.UNKNOWN);
  }

  public PathMetadata(FileStatus fileStatus, Tristate isEmptyDir) {
    this(fileStatus, isEmptyDir, false);
  }

  public PathMetadata(FileStatus fileStatus, Tristate isEmptyDir, boolean
      isDeleted) {
    Preconditions.checkNotNull(fileStatus, "fileStatus must be non-null");
    Preconditions.checkNotNull(fileStatus.getPath(), "fileStatus path must be" +
        " non-null");
    Preconditions.checkArgument(fileStatus.getPath().isAbsolute(), "path must" +
        " be absolute");
    this.fileStatus = fileStatus;
    this.isEmptyDirectory = isEmptyDir;
    this.isDeleted = isDeleted;
  }

  /**
   * @return {@code FileStatus} contained in this {@code PathMetadata}.
   */
  public final FileStatus getFileStatus() {
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
        '}';
  }

  /**
   * Log contents to supplied StringBuilder in a pretty fashion.
   * @param sb target StringBuilder
   */
  public void prettyPrint(StringBuilder sb) {
    sb.append(String.format("%-5s %-20s %-7d %-8s %-6s",
        fileStatus.isDirectory() ? "dir" : "file",
        fileStatus.getPath().toString(), fileStatus.getLen(),
        isEmptyDirectory.name(), isDeleted));
    sb.append(fileStatus);
  }

  public String prettyPrint() {
    StringBuilder sb = new StringBuilder();
    prettyPrint(sb);
    return sb.toString();
  }
}
