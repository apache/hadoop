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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.marshallPath;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.unmarshallPath;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.verify;

/**
 * A directory entry in the task manifest.
 * Uses shorter field names for smaller files.
 * Hash and equals are on dir name only.
 * Can be serialized as a java object, json object
 * or hadoop writable.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class DirEntry implements Serializable, Writable {

  private static final long serialVersionUID = 5658520530209859765L;

  /**
   * Destination directory.
   */
  @JsonProperty("d")
  private String dir;

  /**
   * Type of dest entry as found when probed for in task commit.
   */
  @JsonProperty("t")
  private int type;

  /**
   * Level in the treewalk.
   */
  @JsonProperty("l")
  private int level;

  /**
   * Constructor for use by jackson/writable.
   * Do Not Delete.
   */
  private DirEntry() {
  }

  /**
   * Construct an entry.
   *
   * @param dir destination path.
   * @param type type of dest entry
   * @param level Level in the treewalk.
   *
   */
  public DirEntry(
      final String dir,
      final int type,
      final int level) {
    this.dir = requireNonNull(dir);
    this.type = type;
    this.level = level;
  }

  /**
   * Construct an entry.
   *
   * @param dir destination path.
   * @param type type of dest entry
   * @param level Level in the treewalk.
   *
   */
  public DirEntry(
      final Path dir,
      final int type,
      final int level) {
    this(marshallPath(dir), type, level);
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

  public void setLevel(final int level) {
    this.level = level;
  }

  public int getLevel() {
    return level;
  }

  @JsonIgnore
  public EntryStatus getStatus() {
    return EntryStatus.toEntryStatus(type);
  }

  @JsonIgnore
  public void setStatus(EntryStatus status) {
    setType(status.ordinal());
  }
  public void validate() throws IOException {
    final String s = toString();
    verify(dir != null && dir.length() > 0,
        "destination path is missing from " + s);
    verify(type >= 0,
        "Invalid type in " + s);
    verify(level >= 0,
        "Invalid level in " + s);
  }

  @Override
  public String toString() {
    return "DirEntry{" +
        "dir='" + dir + '\'' +
        ", type=" + type +
        ", level=" + level +
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
    return dir.equals(dirEntry.dir);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dir);
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeUTF(dir);
    out.writeInt(type);
    out.writeInt(level);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    dir = in.readUTF();
    type = in.readInt();
    level = in.readInt();
  }

  /**
   * A directory entry.
   * @param dest destination path.
   * @param type type
   * @param level Level in the treewalk.
   * @return an entry
   */
  public static DirEntry dirEntry(Path dest, int type, int level) {
    return new DirEntry(dest, type, level);
  }

  /**
   * A directory entry.
   * @param dest destination path.
   * @param type type
   * @param level Level in the treewalk.
   * @return an entry
   */
  public static DirEntry dirEntry(Path dest, EntryStatus type, int level) {
    return dirEntry(dest, type.ordinal(), level);
  }

}
