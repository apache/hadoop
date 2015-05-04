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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Describes a path-based cache directive.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Public
public class CacheDirectiveStats {
  public static class Builder {
    private long bytesNeeded;
    private long bytesCached;
    private long filesNeeded;
    private long filesCached;
    private boolean hasExpired;

    /**
     * Builds a new CacheDirectiveStats populated with the set properties.
     * 
     * @return New CacheDirectiveStats.
     */
    public CacheDirectiveStats build() {
      return new CacheDirectiveStats(bytesNeeded, bytesCached, filesNeeded,
          filesCached, hasExpired);
    }

    /**
     * Creates an empty builder.
     */
    public Builder() {
    }

    /**
     * Sets the bytes needed by this directive.
     * 
     * @param bytesNeeded The bytes needed.
     * @return This builder, for call chaining.
     */
    public Builder setBytesNeeded(long bytesNeeded) {
      this.bytesNeeded = bytesNeeded;
      return this;
    }

    /**
     * Sets the bytes cached by this directive.
     * 
     * @param bytesCached The bytes cached.
     * @return This builder, for call chaining.
     */
    public Builder setBytesCached(long bytesCached) {
      this.bytesCached = bytesCached;
      return this;
    }

    /**
     * Sets the files needed by this directive.
     * @param filesNeeded The number of files needed
     * @return This builder, for call chaining.
     */
    public Builder setFilesNeeded(long filesNeeded) {
      this.filesNeeded = filesNeeded;
      return this;
    }

    /**
     * Sets the files cached by this directive.
     * 
     * @param filesCached The number of files cached.
     * @return This builder, for call chaining.
     */
    public Builder setFilesCached(long filesCached) {
      this.filesCached = filesCached;
      return this;
    }

    /**
     * Sets whether this directive has expired.
     * 
     * @param hasExpired if this directive has expired
     * @return This builder, for call chaining.
     */
    public Builder setHasExpired(boolean hasExpired) {
      this.hasExpired = hasExpired;
      return this;
    }
  }

  private final long bytesNeeded;
  private final long bytesCached;
  private final long filesNeeded;
  private final long filesCached;
  private final boolean hasExpired;

  private CacheDirectiveStats(long bytesNeeded, long bytesCached,
      long filesNeeded, long filesCached, boolean hasExpired) {
    this.bytesNeeded = bytesNeeded;
    this.bytesCached = bytesCached;
    this.filesNeeded = filesNeeded;
    this.filesCached = filesCached;
    this.hasExpired = hasExpired;
  }

  /**
   * @return The bytes needed.
   */
  public long getBytesNeeded() {
    return bytesNeeded;
  }

  /**
   * @return The bytes cached.
   */
  public long getBytesCached() {
    return bytesCached;
  }

  /**
   * @return The number of files needed.
   */
  public long getFilesNeeded() {
    return filesNeeded;
  }

  /**
   * @return The number of files cached.
   */
  public long getFilesCached() {
    return filesCached;
  }

  /**
   * @return Whether this directive has expired.
   */
  public boolean hasExpired() {
    return hasExpired;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    builder.append("bytesNeeded: ").append(bytesNeeded);
    builder.append(", ").append("bytesCached: ").append(bytesCached);
    builder.append(", ").append("filesNeeded: ").append(filesNeeded);
    builder.append(", ").append("filesCached: ").append(filesCached);
    builder.append(", ").append("hasExpired: ").append(hasExpired);
    builder.append("}");
    return builder.toString();
  }
};
