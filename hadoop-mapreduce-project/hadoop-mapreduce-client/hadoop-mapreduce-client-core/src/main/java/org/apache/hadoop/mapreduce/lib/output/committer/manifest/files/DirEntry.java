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
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.marshallPath;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.unmarshallPath;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.verify;

/**
 * A directory entry in the task manifest.
 * Uses shorter field names for smaller files.
 */

@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class DirEntry implements Serializable {

  private static final long serialVersionUID = 5658520530209859765L;

  /**
   * Destination directory.
   */
  @JsonProperty("d")
  private String dir;

  /**
   * Type of dir as found in task committer.
   */
  @JsonProperty("t")
  private int type;

  public DirEntry() {
  }

  /**
   * Construct an entry.
   * @param source source path.
   * @param etag optional etag
   * @param dir destination path.
   * @param type file size.
   */
  public DirEntry(
      final String dir,
      final int type) {
    this.dir = dir;
    this.type = type;
  }


  /**
   * Construct an entry.
   * @param source source path.
   * @param dir destination path.
   * @param type file size.
   * @param etag optional etag
   */
  public DirEntry(
      final Path dir,
      final int type) {
    this(marshallPath(dir), type);
  }

  public void setDir(final String dir) {
    this.dir = dir;
  }

  public String getDir() {
    return dir;
  }

  @JsonIgnore
  public Path getDestPath() {
    return unmarshallPath(dir);
  }

  public int getType() {
    return type;
  }

  public void setType(final int type) {
    this.type = type;
  }

  @JsonIgnore
  public Status getStatus() {
    return toStatus(type);
  }

  public void validate() throws IOException {
    final String s = toString();
    verify(dir != null && dir.length() > 0,
        "Source is missing from " + s);
    verify(type >= 0,
        "Invalid type in " + s);
  }

  @Override
  public String toString() {
    return "DirEntry{" +
        "dir='" + dir + '\'' +
        ", type=" + type +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DirEntry dirEntry = (DirEntry) o;
    return type == dirEntry.type && dir.equals(dirEntry.dir);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dir, type);
  }

  /**
   * A directory entry.
   * @param dest destination path.
   * @param type type
   * @return an entry with the given source and type
   */
  public static DirEntry dirEntry(Path dest, int type) {
    return new DirEntry(dest, type);
  }

  /**
   * A directory entry.
   * @param dest destination path.
   * @param type type
   * @return an entry with the given source and type
   */
  public static DirEntry dirEntry(Path dest, Status type) {
    return new DirEntry(dest, type.value);
  }

  /**
   * Status.
   */
  public enum Status {

    unknown(0),
    file(1),
    isdir(2),
    created(3);

    private final int value;

    Status(final int value) {
      this.value = value;
    }

  }

  /**
   * Go from a marshalled type to a status value.
   * Any out of range value is converted to unknown.
   * @param type type
   * @return the status value.
   */
  static Status toStatus(int type) {
    switch (type) {
    case 1:
      return Status.file;
    case 2:
      return Status.isdir;
    case 3:
      return Status.created;
    default:
      return Status.unknown;

    }
  }
}
