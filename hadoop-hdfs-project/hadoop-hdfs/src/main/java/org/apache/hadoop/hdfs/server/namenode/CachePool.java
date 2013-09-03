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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * The NameNode uses CachePools to manage cache resources on the DataNodes.
 */
public final class CachePool {
  public static final Log LOG = LogFactory.getLog(CachePool.class);

  @Nonnull
  private final String poolName;

  @Nonnull
  private String ownerName;

  @Nonnull
  private String groupName;
  
  private int mode;
  
  private int weight;
  
  public static String getCurrentUserPrimaryGroupName() throws IOException {
    UserGroupInformation ugi= NameNode.getRemoteUser();
    String[] groups = ugi.getGroupNames();
    if (groups.length == 0) {
      throw new IOException("failed to get group names from UGI " + ugi);
    }
    return groups[0];
  }
  
  public CachePool(String poolName, String ownerName, String groupName,
      Integer mode, Integer weight) throws IOException {
    this.poolName = poolName;
    this.ownerName = ownerName != null ? ownerName :
      NameNode.getRemoteUser().getShortUserName();
    this.groupName = groupName != null ? groupName :
      getCurrentUserPrimaryGroupName();
    this.mode = mode != null ? mode : 0644;
    this.weight = weight != null ? weight : 100;
  }

  public String getName() {
    return poolName;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public CachePool setOwnerName(String ownerName) {
    this.ownerName = ownerName;
    return this;
  }

  public String getGroupName() {
    return groupName;
  }

  public CachePool setGroupName(String groupName) {
    this.groupName = groupName;
    return this;
  }

  public int getMode() {
    return mode;
  }

  public CachePool setMode(int mode) {
    this.mode = mode;
    return this;
  }
  
  public int getWeight() {
    return weight;
  }

  public CachePool setWeight(int weight) {
    this.weight = weight;
    return this;
  }
  
  /**
   * Get information about this cache pool.
   *
   * @param fullInfo
   *          If true, only the name will be returned (i.e., what you 
   *          would get if you didn't have read permission for this pool.)
   * @return
   *          Cache pool information.
   */
  public CachePoolInfo getInfo(boolean fullInfo) {
    CachePoolInfo info = new CachePoolInfo(poolName);
    if (!fullInfo) {
      return info;
    }
    return info.setOwnerName(ownerName).
        setGroupName(groupName).
        setMode(mode).
        setWeight(weight);
  }

  public CachePoolInfo getInfo(FSPermissionChecker pc) {
    return getInfo(pc.checkReadPermission(ownerName, groupName, mode));
  }

  public String toString() {
    return new StringBuilder().
        append("{ ").append("poolName:").append(poolName).
        append(", ownerName:").append(ownerName).
        append(", groupName:").append(groupName).
        append(", mode:").append(String.format("%3o", mode)).
        append(", weight:").append(weight).
        append(" }").toString();
  }
}
