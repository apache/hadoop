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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * CachePoolInfo describes a cache pool.
 *
 * This class is used in RPCs to create and modify cache pools.
 * It is serializable and can be stored in the edit log.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CachePoolInfo {
  public static final Log LOG = LogFactory.getLog(CachePoolInfo.class);

  final String poolName;

  @Nullable
  String ownerName;

  @Nullable
  String groupName;

  @Nullable
  FsPermission mode;

  @Nullable
  Long limit;

  public CachePoolInfo(String poolName) {
    this.poolName = poolName;
  }
  
  public String getPoolName() {
    return poolName;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public CachePoolInfo setOwnerName(String ownerName) {
    this.ownerName = ownerName;
    return this;
  }

  public String getGroupName() {
    return groupName;
  }

  public CachePoolInfo setGroupName(String groupName) {
    this.groupName = groupName;
    return this;
  }
  
  public FsPermission getMode() {
    return mode;
  }

  public CachePoolInfo setMode(FsPermission mode) {
    this.mode = mode;
    return this;
  }

  public Long getLimit() {
    return limit;
  }

  public CachePoolInfo setLimit(Long bytes) {
    this.limit = bytes;
    return this;
  }

  public String toString() {
    return new StringBuilder().append("{").
      append("poolName:").append(poolName).
      append(", ownerName:").append(ownerName).
      append(", groupName:").append(groupName).
      append(", mode:").append((mode == null) ? "null" :
          String.format("0%03o", mode.toShort())).
      append(", limit:").append(limit).
      append("}").toString();
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
        hashCode();
  }

  public static void validate(CachePoolInfo info) throws IOException {
    if (info == null) {
      throw new InvalidRequestException("CachePoolInfo is null");
    }
    if ((info.getLimit() != null) && (info.getLimit() < 0)) {
      throw new InvalidRequestException("Limit is negative.");
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
