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

import javax.annotation.Nullable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo.Expiration;

/**
 * CachePoolInfo describes a cache pool.
 *
 * This class is used in RPCs to create and modify cache pools.
 * It is serializable and can be stored in the edit log.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CachePoolInfo {

  /**
   * Indicates that the pool does not have a maximum relative expiry.
   */
  public static final long RELATIVE_EXPIRY_NEVER =
      Expiration.MAX_RELATIVE_EXPIRY_MS;
  /**
   * Default max relative expiry for cache pools.
   */
  public static final long DEFAULT_MAX_RELATIVE_EXPIRY =
      RELATIVE_EXPIRY_NEVER;

  public static final long LIMIT_UNLIMITED = Long.MAX_VALUE;
  public static final long DEFAULT_LIMIT = LIMIT_UNLIMITED;

  public static final short DEFAULT_REPLICATION_NUM = 1;

  final String poolName;

  @Nullable
  String ownerName;

  @Nullable
  String groupName;

  @Nullable
  FsPermission mode;

  @Nullable
  Long limit;

  @Nullable
  private Short defaultReplication;

  @Nullable
  Long maxRelativeExpiryMs;

  public CachePoolInfo(String poolName) {
    this.poolName = poolName;
  }

  /**
   * @return Name of the pool.
   */
  public String getPoolName() {
    return poolName;
  }

  /**
   * @return The owner of the pool. Along with the group and mode, determines
   *         who has access to view and modify the pool.
   */
  public String getOwnerName() {
    return ownerName;
  }

  public CachePoolInfo setOwnerName(String ownerName) {
    this.ownerName = ownerName;
    return this;
  }

  /**
   * @return The group of the pool. Along with the owner and mode, determines
   *         who has access to view and modify the pool.
   */
  public String getGroupName() {
    return groupName;
  }

  public CachePoolInfo setGroupName(String groupName) {
    this.groupName = groupName;
    return this;
  }

  /**
   * @return Unix-style permissions of the pool. Along with the owner and group,
   *         determines who has access to view and modify the pool.
   */
  public FsPermission getMode() {
    return mode;
  }

  public CachePoolInfo setMode(FsPermission mode) {
    this.mode = mode;
    return this;
  }

  /**
   * @return The maximum aggregate number of bytes that can be cached by
   *         directives in this pool.
   */
  public Long getLimit() {
    return limit;
  }

  public CachePoolInfo setLimit(Long bytes) {
    this.limit = bytes;
    return this;
  }

  /**
   * @return The default replication num for CacheDirective in this pool
     */
  public Short getDefaultReplication() {
    return defaultReplication;
  }

  public CachePoolInfo setDefaultReplication(Short repl) {
    this.defaultReplication = repl;
    return this;
  }

  /**
   * @return The maximum relative expiration of directives of this pool in
   *         milliseconds
   */
  public Long getMaxRelativeExpiryMs() {
    return maxRelativeExpiryMs;
  }

  /**
   * Set the maximum relative expiration of directives of this pool in
   * milliseconds.
   *
   * @param ms in milliseconds
   * @return This builder, for call chaining.
   */
  public CachePoolInfo setMaxRelativeExpiryMs(Long ms) {
    this.maxRelativeExpiryMs = ms;
    return this;
  }

  public String toString() {
    return "{" + "poolName:" + poolName
        + ", ownerName:" + ownerName
        + ", groupName:" + groupName
        + ", mode:"
        + ((mode == null) ? "null" : String.format("0%03o", mode.toShort()))
        + ", limit:" + limit
        + ", defaultReplication:" + defaultReplication
        + ", maxRelativeExpiryMs:" + maxRelativeExpiryMs + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) { return false; }
    if (o == this) { return true; }
    if (o.getClass() != getClass()) {
      return false;
    }
    CachePoolInfo other = (CachePoolInfo)o;
    return new EqualsBuilder().
        append(poolName, other.poolName).
        append(ownerName, other.ownerName).
        append(groupName, other.groupName).
        append(mode, other.mode).
        append(limit, other.limit).
        append(defaultReplication, other.defaultReplication).
        append(maxRelativeExpiryMs, other.maxRelativeExpiryMs).
        isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(poolName).
        append(ownerName).
        append(groupName).
        append(mode).
        append(limit).
        append(defaultReplication).
        append(maxRelativeExpiryMs).
        hashCode();
  }

  public static void validate(CachePoolInfo info) throws IOException {
    if (info == null) {
      throw new InvalidRequestException("CachePoolInfo is null");
    }
    if ((info.getLimit() != null) && (info.getLimit() < 0)) {
      throw new InvalidRequestException("Limit is negative.");
    }
    if ((info.getDefaultReplication() != null)
            && (info.getDefaultReplication() < 0)) {
      throw new InvalidRequestException("Default Replication is negative");
    }

    if (info.getMaxRelativeExpiryMs() != null) {
      long maxRelativeExpiryMs = info.getMaxRelativeExpiryMs();
      if (maxRelativeExpiryMs < 0l) {
        throw new InvalidRequestException("Max relative expiry is negative.");
      }
      if (maxRelativeExpiryMs > Expiration.MAX_RELATIVE_EXPIRY_MS) {
        throw new InvalidRequestException("Max relative expiry is too big.");
      }
    }
    validateName(info.poolName);
  }

  public static void validateName(String poolName) throws IOException {
    if (poolName == null || poolName.isEmpty()) {
      // Empty pool names are not allowed because they would be highly
      // confusing.  They would also break the ability to list all pools
      // by starting with prevKey = ""
      throw new IOException("invalid empty cache pool name");
    }
  }
}
