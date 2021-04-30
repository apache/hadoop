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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import java.io.IOException;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.marshallPath;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.unmarshallPath;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.verify;

/**
 * A File or directory entry in the task manifest.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class FileOrDirEntry implements Serializable {

  private static final long serialVersionUID = -550288489009777867L;

  @JsonProperty("source")
  private String source;

  @JsonProperty("dest")
  private String dest;

  @JsonProperty("size")
  private long size;

  public FileOrDirEntry() {
  }

  /**
   * Construct an entry.
   * @param source source path.
   * @param dest destination path.
   * @param size file size.
   */
  public FileOrDirEntry(
      final String source,
      final String dest,
      final long size) {
    this.source = source;
    this.dest = dest;
    this.size = size;
  }


  /**
   * Construct an entry.
   * @param source source path.
   * @param dest destination path.
   * @param size file size.
   */
  public FileOrDirEntry(
      final Path source,
      final Path dest,
      final long size) {
    this.source = marshallPath(source);
    this.dest = marshallPath(dest);
    this.size = size;
  }


  public void setSource(final String source) {
    this.source = source;
  }

  public String getSource() {
    return source;
  }

  @JsonIgnore
  public Path getSourcePath() {
    return unmarshallPath(source);
  }

  public void setDest(final String dest) {
    this.dest = dest;
  }

  public String getDest() {
    return dest;
  }

  @JsonIgnore
  public Path getDestPath() {
    return unmarshallPath(dest);
  }

  public long getSize() {
    return size;
  }

  public void setSize(final long size) {
    this.size = size;
  }

  public void validate() throws IOException {
    final String s = toString();
    verify(source != null && source.length() > 0,
        "Source is missing from " + s);
    verify(dest != null && dest.length() > 0,
        "Source is missing from " + s);
    verify(size >= 0,
        "Invalid size in " + s);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "FileOrDirEntry{");
    sb.append("source='").append(source).append('\'');
    sb.append(", dest='").append(dest).append('\'');
    sb.append(", size=").append(size);
    sb.append('}');
    return sb.toString();
  }

  /**
   * A directory entry.
   * @param src source path
   * @param dest destination path.
   * @return an entry with the given source and destination, no file.
   */
  public static FileOrDirEntry dirEntry(Path src, Path dest) {
    return new FileOrDirEntry(src,
        dest, 0);
  }

  /**
   * Create an entry for a file to rename under the destination.
   * @param status source file
   * @param destDir destination directory
   * @return an entry which includes the rename path
   */
  public static FileOrDirEntry fileEntry(FileStatus status, Path destDir) {
    // generate a new path under the dest dir
    Path dest = new Path(destDir, status.getPath().getName());
    return new FileOrDirEntry(status.getPath(),
        dest,
        status.getLen());
  }

}
