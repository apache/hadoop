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

import java.util.Date;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.DFSUtilClient;

/**
 * Describes a path-based cache directive.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Public
public class CacheDirectiveInfo {
  /**
   * A builder for creating new CacheDirectiveInfo instances.
   */
  public static class Builder {
    private Long id;
    private Path path;
    private Short replication;
    private String pool;
    private Expiration expiration;

    /**
     * Builds a new CacheDirectiveInfo populated with the set properties.
     *
     * @return New CacheDirectiveInfo.
     */
    public CacheDirectiveInfo build() {
      return new CacheDirectiveInfo(id, path, replication, pool, expiration);
    }

    /**
     * Creates an empty builder.
     */
    public Builder() {
    }

    /**
     * Creates a builder with all elements set to the same values as the
     * given CacheDirectiveInfo.
     */
    public Builder(CacheDirectiveInfo directive) {
      this.id = directive.getId();
      this.path = directive.getPath();
      this.replication = directive.getReplication();
      this.pool = directive.getPool();
      this.expiration = directive.getExpiration();
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

    /**
     * Sets when the CacheDirective should expire. A
     * {@link CacheDirectiveInfo.Expiration} can specify either an absolute or
     * relative expiration time.
     *
     * @param expiration when this CacheDirective should expire
     * @return This builder, for call chaining
     */
    public Builder setExpiration(Expiration expiration) {
      this.expiration = expiration;
      return this;
    }
  }

  /**
   * Denotes a relative or absolute expiration time for a CacheDirective. Use
   * factory methods {@link CacheDirectiveInfo.Expiration#newAbsolute(Date)} and
   * {@link CacheDirectiveInfo.Expiration#newRelative(long)} to create an
   * Expiration.
   * <p>
   * In either case, the server-side clock is used to determine when a
   * CacheDirective expires.
   */
  public static class Expiration {

    /**
     * The maximum value we accept for a relative expiry.
     */
    public static final long MAX_RELATIVE_EXPIRY_MS =
        Long.MAX_VALUE / 4; // This helps prevent weird overflow bugs

    /**
     * An relative Expiration that never expires.
     */
    public static final Expiration NEVER = newRelative(MAX_RELATIVE_EXPIRY_MS);

    /**
     * Create a new relative Expiration.
     * <p>
     * Use {@link Expiration#NEVER} to indicate an Expiration that never
     * expires.
     *
     * @param ms how long until the CacheDirective expires, in milliseconds
     * @return A relative Expiration
     */
    public static Expiration newRelative(long ms) {
      return new Expiration(ms, true);
    }

    /**
     * Create a new absolute Expiration.
     * <p>
     * Use {@link Expiration#NEVER} to indicate an Expiration that never
     * expires.
     *
     * @param date when the CacheDirective expires
     * @return An absolute Expiration
     */
    public static Expiration newAbsolute(Date date) {
      return new Expiration(date.getTime(), false);
    }

    /**
     * Create a new absolute Expiration.
     * <p>
     * Use {@link Expiration#NEVER} to indicate an Expiration that never
     * expires.
     *
     * @param ms when the CacheDirective expires, in milliseconds since the Unix
     *          epoch.
     * @return An absolute Expiration
     */
    public static Expiration newAbsolute(long ms) {
      return new Expiration(ms, false);
    }

    private final long ms;
    private final boolean isRelative;

    private Expiration(long ms, boolean isRelative) {
      if (isRelative) {
        Preconditions.checkArgument(ms <= MAX_RELATIVE_EXPIRY_MS,
            "Expiration time is too far in the future!");
      }
      this.ms = ms;
      this.isRelative = isRelative;
    }

    /**
     * @return true if Expiration was specified as a relative duration, false if
     *         specified as an absolute time.
     */
    public boolean isRelative() {
      return isRelative;
    }

    /**
     * @return The raw underlying millisecond value, either a relative duration
     *         or an absolute time as milliseconds since the Unix epoch.
     */
    public long getMillis() {
      return ms;
    }

    /**
     * @return Expiration time as a {@link Date} object. This converts a
     *         relative Expiration into an absolute Date based on the local
     *         clock.
     */
    public Date getAbsoluteDate() {
      return new Date(getAbsoluteMillis());
    }

    /**
     * @return Expiration time in milliseconds from the Unix epoch. This
     *         converts a relative Expiration into an absolute time based on the
     *         local clock.
     */
    public long getAbsoluteMillis() {
      if (!isRelative) {
        return ms;
      } else {
        return new Date().getTime() + ms;
      }
    }

    @Override
    public String toString() {
      if (isRelative) {
        return DFSUtilClient.durationToString(ms);
      }
      return DFSUtilClient.dateToIso8601String(new Date(ms));
    }
  }

  private final Long id;
  private final Path path;
  private final Short replication;
  private final String pool;
  private final Expiration expiration;

  CacheDirectiveInfo(Long id, Path path, Short replication, String pool,
      Expiration expiration) {
    this.id = id;
    this.path = path;
    this.replication = replication;
    this.pool = pool;
    this.expiration = expiration;
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

  /**
   * @return When this directive expires.
   */
  public Expiration getExpiration() {
    return expiration;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    CacheDirectiveInfo other = (CacheDirectiveInfo)o;
    return new EqualsBuilder().append(getId(), other.getId()).
        append(getPath(), other.getPath()).
        append(getReplication(), other.getReplication()).
        append(getPool(), other.getPool()).
        append(getExpiration(), other.getExpiration()).
        isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(id).
        append(path).
        append(replication).
        append(pool).
        append(expiration).
        hashCode();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    String prefix = "";
    if (id != null) {
      builder.append(prefix).append("id: ").append(id);
      prefix = ", ";
    }
    if (path != null) {
      builder.append(prefix).append("path: ").append(path);
      prefix = ", ";
    }
    if (replication != null) {
      builder.append(prefix).append("replication: ").append(replication);
      prefix = ", ";
    }
    if (pool != null) {
      builder.append(prefix).append("pool: ").append(pool);
      prefix = ", ";
    }
    if (expiration != null) {
      builder.append(prefix).append("expiration: ").append(expiration);
    }
    builder.append("}");
    return builder.toString();
  }
}
