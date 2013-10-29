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

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.InvalidPoolNameError;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.InvalidPathNameError;

/**
 * A directive to add a path to a cache pool.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Public
public class PathBasedCacheDirective {

  /**
   * A builder for creating new PathBasedCacheDirective instances.
   */
  public static class Builder {
    private Path path;
    private short replication = (short)1;
    private String pool;

    /**
     * Builds a new PathBasedCacheDirective populated with the set properties.
     * 
     * @return New PathBasedCacheDirective.
     */
    public PathBasedCacheDirective build() {
      return new PathBasedCacheDirective(path, replication, pool);
    }

    /**
     * Sets the path used in this request.
     * 
     * @param path The path used in this request.
     * @return This builder, for call chaining.
     */
    public Builder setPath(Path path) {
      this.path = path;
      return this;
    }

    /**
     * Sets the replication used in this request.
     * 
     * @param replication The replication used in this request.
     * @return This builder, for call chaining.
     */
    public Builder setReplication(short replication) {
      this.replication = replication;
      return this;
    }

    /**
     * Sets the pool used in this request.
     * 
     * @param pool The pool used in this request.
     * @return This builder, for call chaining.
     */
    public Builder setPool(String pool) {
      this.pool = pool;
      return this;
    }
  }

  private final Path path;
  private final short replication;
  private final String pool;

  /**
   * @return The path used in this request.
   */
  public Path getPath() {
    return path;
  }

  /**
   * @return The number of times the block should be cached.
   */
  public short getReplication() {
    return replication;
  }

  /**
   * @return The pool used in this request.
   */
  public String getPool() {
    return pool;
  }

  /**
   * Check if this PathBasedCacheDirective is valid.
   * 
   * @throws IOException
   *     If this PathBasedCacheDirective is not valid.
   */
  public void validate() throws IOException {
    if (!DFSUtil.isValidName(path.toUri().getPath())) {
      throw new InvalidPathNameError(this);
    }
    if (replication <= 0) {
      throw new IOException("Tried to request a cache replication " +
          "factor of " + replication + ", but that is less than 1.");
    }
    if (pool.isEmpty()) {
      throw new InvalidPoolNameError(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    PathBasedCacheDirective other = (PathBasedCacheDirective)o;
    return new EqualsBuilder().append(getPath(), other.getPath()).
        append(getReplication(), other.getReplication()).
        append(getPool(), other.getPool()).
        isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(getPath()).
        append(replication).
        append(getPool()).
        hashCode();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ path:").append(path).
      append(", replication:").append(replication).
      append(", pool:").append(pool).
      append(" }");
    return builder.toString();
  }

  /**
   * Protected constructor.  Callers use Builder to create new instances.
   * 
   * @param path The path used in this request.
   * @param replication The replication used in this request.
   * @param pool The pool used in this request.
   */
  protected PathBasedCacheDirective(Path path, short replication, String pool) {
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(pool);
    this.path = path;
    this.replication = replication;
    this.pool = pool;
  }
};
