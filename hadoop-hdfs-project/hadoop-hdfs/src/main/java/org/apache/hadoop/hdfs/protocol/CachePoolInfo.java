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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Information about a cache pool.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CachePoolInfo {
  final String poolName;

  @Nullable
  String ownerName;

  @Nullable
  String groupName;

  @Nullable
  FsPermission mode;

  @Nullable
  Integer weight;

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

  public Integer getWeight() {
    return weight;
  }

  public CachePoolInfo setWeight(Integer weight) {
    this.weight = weight;
    return this;
  }

  public String toString() {
    return new StringBuilder().append("{").
      append("poolName:").append(poolName).
      append(", ownerName:").append(ownerName).
      append(", groupName:").append(groupName).
      append(", mode:").append((mode == null) ? "null" :
          String.format("0%03o", mode)).
      append(", weight:").append(weight).
      append("}").toString();
  }
  
  @Override
  public boolean equals(Object o) {
    try {
      CachePoolInfo other = (CachePoolInfo)o;
      return new EqualsBuilder().
          append(poolName, other.poolName).
          append(ownerName, other.ownerName).
          append(groupName, other.groupName).
          append(mode, other.mode).
          append(weight, other.weight).
          isEquals();
    } catch (ClassCastException e) {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(poolName).
        append(ownerName).
        append(groupName).
        append(mode).
        append(weight).
        hashCode();
  }

  public static void validate(CachePoolInfo info) throws IOException {
    if (info == null) {
      throw new IOException("CachePoolInfo is null");
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
