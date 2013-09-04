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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo.Builder;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A CachePool describes a set of cache resources being managed by the NameNode.
 * User caching requests are billed to the cache pool specified in the request.
 *
 * CachePools are uniquely identified by a numeric id as well as the
 * {@link CachePoolInfo} pool name. Mutable metadata is contained in
 * CachePoolInfo, including pool name, owner, group, and permissions.
 * See this class for more details.
 */
public final class CachePool {
  public static final Log LOG = LogFactory.getLog(CachePool.class);

  private final long id;

  private CachePoolInfo info;

  public CachePool(long id) {
    this.id = id;
    this.info = null;
  }

  CachePool(long id, String poolName, String ownerName, String groupName,
      FsPermission mode, Integer weight) throws IOException {
    this.id = id;
    // Set CachePoolInfo default fields if null
    if (poolName == null || poolName.isEmpty()) {
      throw new IOException("invalid empty cache pool name");
    }
    UserGroupInformation ugi = null;
    if (ownerName == null) {
      ugi = NameNode.getRemoteUser();
      ownerName = ugi.getShortUserName();
    }
    if (groupName == null) {
      if (ugi == null) {
        ugi = NameNode.getRemoteUser();
      }
      String[] groups = ugi.getGroupNames();
      if (groups.length == 0) {
        throw new IOException("failed to get group names from UGI " + ugi);
      }
      groupName = groups[0];
    }
    if (mode == null) {
      mode = FsPermission.getDirDefault();
    }
    if (weight == null) {
      weight = 100;
    }
    CachePoolInfo.Builder builder = CachePoolInfo.newBuilder();
    builder.setPoolName(poolName).setOwnerName(ownerName)
        .setGroupName(groupName).setMode(mode).setWeight(weight);
    this.info = builder.build();
  }

  public CachePool(long id, CachePoolInfo info) {
    this.id = id;
    this.info = info;
  }

  /**
   * @return id of the pool
   */
  public long getId() {
    return id;
  }

  /**
   * Get information about this cache pool.
   *
   * @return
   *          Cache pool information.
   */
  public CachePoolInfo getInfo() {
    return info;
  }

  void setInfo(CachePoolInfo info) {
    this.info = info;
  }

  public String toString() {
    return new StringBuilder().
        append("{ ").append("id:").append(id).
        append(", info:").append(info.toString()).
        append(" }").toString();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(id).append(info).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    CachePool rhs = (CachePool)obj;
    return new EqualsBuilder()
      .append(id, rhs.id)
      .append(info, rhs.info)
      .isEquals();
  }
}
