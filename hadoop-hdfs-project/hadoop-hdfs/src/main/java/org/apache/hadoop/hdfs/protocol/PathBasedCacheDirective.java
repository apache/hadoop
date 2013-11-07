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

import java.net.URI;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

/**
 * Describes a path-based cache directive.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Public
public class PathBasedCacheDirective {
  /**
   * A builder for creating new PathBasedCacheDirective instances.
   */
  public static class Builder {
    private Long id;
    private Path path;
    private Short replication;
    private String pool;

    /**
     * Builds a new PathBasedCacheDirective populated with the set properties.
     * 
     * @return New PathBasedCacheDirective.
     */
    public PathBasedCacheDirective build() {
      return new PathBasedCacheDirective(id, path, replication, pool);
    }

    /**
     * Creates an empty builder.
     */
    public Builder() {
    }

    /**
     * Creates a builder with all elements set to the same values as the
     * given PathBasedCacheDirective.
     */
    public Builder(PathBasedCacheDirective directive) {
      this.id = directive.getId();
      // deep-copy URI
      URI uri = directive.getPath().toUri();
      this.path = new Path(uri.getScheme(), uri.getAuthority(), uri.getPath());
      this.replication = directive.getReplication();
      this.pool = directive.getPool();
    }

    /**
     * Sets the id used in this request.
     * 
     * @param id The id used in this request.
     * @return This builder, for call chaining.
     */
    public Builder setId(Long id) {
      this.id = id;
      return this;
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
    public Builder setReplication(Short replication) {
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

  private final Long id;
  private final Path path;
  private final Short replication;
  private final String pool;

  PathBasedCacheDirective(Long id, Path path, Short replication, String pool) {
    this.id = id;
    this.path = path;
    this.replication = replication;
    this.pool = pool;
  }

  /**
   * @return The ID of this directive.
   */
  public Long getId() {
    return id;
  }

  /**
   * @return The path used in this request.
   */
  public Path getPath() {
    return path;
  }

  /**
   * @return The number of times the block should be cached.
   */
  public Short getReplication() {
    return replication;
  }

  /**
   * @return The pool used in this request.
   */
  public String getPool() {
    return pool;
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
    return new EqualsBuilder().append(getId(), other.getId()).
        append(getPath(), other.getPath()).
        append(getReplication(), other.getReplication()).
        append(getPool(), other.getPool()).
        isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(id).
        append(path).
        append(replication).
        append(pool).
        hashCode();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    String prefix = "";
    if (id != null) {
      builder.append(prefix).append("id: ").append(id);
      prefix = ",";
    }
    if (path != null) {
      builder.append(prefix).append("path: ").append(path);
      prefix = ",";
    }
    if (replication != null) {
      builder.append(prefix).append("replication: ").append(replication);
      prefix = ",";
    }
    if (pool != null) {
      builder.append(prefix).append("pool: ").append(pool);
      prefix = ",";
    }
    builder.append("}");
    return builder.toString();
  }
};
