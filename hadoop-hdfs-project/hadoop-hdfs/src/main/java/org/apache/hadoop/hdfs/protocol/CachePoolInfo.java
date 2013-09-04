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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;

import com.google.common.base.Preconditions;

/**
 * Information about a cache pool.
 * 
 * CachePoolInfo permissions roughly map to Unix file permissions.
 * Write permissions allow addition and removal of a {@link PathCacheEntry} from
 * the pool. Execute permissions allow listing of PathCacheEntries in a pool.
 * Read permissions have no associated meaning.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CachePoolInfo {

  private String poolName;
  private String ownerName;
  private String groupName;
  private FsPermission mode;
  private Integer weight;

  /**
   * For Builder use
   */
  private CachePoolInfo() {}

  /**
   * Use a CachePoolInfo {@link Builder} to create a new CachePoolInfo with
   * more parameters
   */
  public CachePoolInfo(String poolName) {
    this.poolName = poolName;
  }

  public String getPoolName() {
    return poolName;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public String getGroupName() {
    return groupName;
  }

  public FsPermission getMode() {
    return mode;
  }

  public Integer getWeight() {
    return weight;
  }

  public String toString() {
    return new StringBuilder().
        append("{ ").append("poolName:").append(poolName).
        append(", ownerName:").append(ownerName).
        append(", groupName:").append(groupName).
        append(", mode:").append(mode).
        append(", weight:").append(weight).
        append(" }").toString();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(poolName).append(ownerName)
        .append(groupName).append(mode.toShort()).append(weight).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    CachePoolInfo rhs = (CachePoolInfo)obj;
    return new EqualsBuilder()
      .append(poolName, rhs.poolName)
      .append(ownerName, rhs.ownerName)
      .append(groupName, rhs.groupName)
      .append(mode, rhs.mode)
      .append(weight, rhs.weight)
      .isEquals();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(CachePoolInfo info) {
    return new Builder(info);
  }

  /**
   * CachePoolInfo Builder
   */
  public static class Builder {
    private CachePoolInfo info;

    public Builder() {
      this.info = new CachePoolInfo();
    }

    public Builder(CachePoolInfo info) {
      this.info = info;
    }

    public CachePoolInfo build() {
      Preconditions.checkNotNull(info.poolName,
          "Cannot create a CachePoolInfo without a pool name");
      return info;
    }

    public Builder setPoolName(String poolName) {
      info.poolName = poolName;
      return this;
    }

    public Builder setOwnerName(String ownerName) {
      info.ownerName = ownerName;
      return this;
    }

    public Builder setGroupName(String groupName) {
      info.groupName = groupName;
      return this;
    }

    public Builder setMode(FsPermission mode) {
      info.mode = mode;
      return this;
    }

    public Builder setWeight(Integer weight) {
      info.weight = weight;
      return this;
    }
  }

}