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
    private long filesAffected;

    /**
     * Builds a new CacheDirectiveStats populated with the set properties.
     * 
     * @return New CacheDirectiveStats.
     */
    public CacheDirectiveStats build() {
      return new CacheDirectiveStats(bytesNeeded, bytesCached, filesAffected);
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
    public Builder setBytesNeeded(Long bytesNeeded) {
      this.bytesNeeded = bytesNeeded;
      return this;
    }

    /**
     * Sets the bytes cached by this directive.
     * 
     * @param bytesCached The bytes cached.
     * @return This builder, for call chaining.
     */
    public Builder setBytesCached(Long bytesCached) {
      this.bytesCached = bytesCached;
      return this;
    }

    /**
     * Sets the files affected by this directive.
     * 
     * @param filesAffected The files affected.
     * @return This builder, for call chaining.
     */
    public Builder setFilesAffected(Long filesAffected) {
      this.filesAffected = filesAffected;
      return this;
    }
  }

  private final long bytesNeeded;
  private final long bytesCached;
  private final long filesAffected;

  private CacheDirectiveStats(long bytesNeeded, long bytesCached,
      long filesAffected) {
    this.bytesNeeded = bytesNeeded;
    this.bytesCached = bytesCached;
    this.filesAffected = filesAffected;
  }

  /**
   * @return The bytes needed.
   */
  public Long getBytesNeeded() {
    return bytesNeeded;
  }

  /**
   * @return The bytes cached.
   */
  public Long getBytesCached() {
    return bytesCached;
  }

  /**
   * @return The files affected.
   */
  public Long getFilesAffected() {
    return filesAffected;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    builder.append("bytesNeeded: ").append(bytesNeeded);
    builder.append(", ").append("bytesCached: ").append(bytesCached);
    builder.append(", ").append("filesAffected: ").append(filesAffected);
    builder.append("}");
    return builder.toString();
  }
};
