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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.marshallPath;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.unmarshallPath;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData.verify;

/**
 * A File entry in the task manifest.
 * Uses shorter field names for smaller files.
 * Used as a Hadoop writable when saved to in intermediate file
 * during job commit.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class FileEntry implements Serializable, Writable {

  private static final long serialVersionUID = -550288489009777867L;

  @JsonProperty("s")
  private String source;

  @JsonProperty("d")
  private String dest;

  @JsonProperty("z")
  private long size;

  /**
   * Etag value if we can extract this.
   */
  @JsonProperty("e")
  private String etag;

  /**
   * Constructor for serialization/deserialization.
   * Do Not Delete.
   */
  public FileEntry() {
  }

  /**
   * Construct an entry.
   * @param source source path.
   * @param dest destination path.
   * @param size file size.
   * @param etag optional etag
   */
  public FileEntry(
      final String source,
      final String dest,
      final long size,
      final String etag) {
    this.source = source;
    this.dest = dest;
    this.size = size;
    this.etag = etag;
  }


  /**
   * Construct an entry.
   * @param source source path.
   * @param dest destination path.
   * @param size file size.
   * @param etag optional etag
   */
  public FileEntry(
      final Path source,
      final Path dest,
      final long size,
      final String etag) {
    this(marshallPath(source), marshallPath(dest), size, etag);
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

  public String getEtag() {
    return etag;
  }

  public void setEtag(final String etag) {
    this.etag = etag;
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
    sb.append(", etag='").append(etag).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileEntry that = (FileEntry) o;
    return size == that.size
        && Objects.equals(source, that.source)
        && Objects.equals(dest, that.dest)
        && Objects.equals(etag, that.etag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, dest);
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    Text.writeString(out, requireNonNull(source, "null source"));
    Text.writeString(out, requireNonNull(dest, "null dest"));
    Text.writeString(out, etag != null ? etag : "");
    WritableUtils.writeVLong(out, size);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    source = Text.readString(in);
    dest = Text.readString(in);
    etag = Text.readString(in);
    size = WritableUtils.readVLong(in);
  }
}
