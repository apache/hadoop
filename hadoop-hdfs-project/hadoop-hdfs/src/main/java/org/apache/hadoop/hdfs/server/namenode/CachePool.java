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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolStats;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.IntrusiveCollection;

import com.google.common.base.Preconditions;

/**
 * A CachePool describes a set of cache resources being managed by the NameNode.
 * User caching requests are billed to the cache pool specified in the request.
 *
 * This is an internal class, only used on the NameNode.  For identifying or
 * describing a cache pool to clients, please use CachePoolInfo.
 * 
 * CachePools must be accessed under the FSNamesystem lock.
 */
@InterfaceAudience.Private
public final class CachePool {
  @Nonnull
  private final String poolName;

  @Nonnull
  private String ownerName;

  @Nonnull
  private String groupName;
  
  /**
   * Cache pool permissions.
   * 
   * READ permission means that you can list the cache directives in this pool.
   * WRITE permission means that you can add, remove, or modify cache directives
   *       in this pool.
   * EXECUTE permission is unused.
   */
  @Nonnull
  private FsPermission mode;

  /**
   * Maximum number of bytes that can be cached in this pool.
   */
  private long limit;

  /**
   * Maximum duration that a CacheDirective in this pool remains valid,
   * in milliseconds.
   */
  private long maxRelativeExpiryMs;

  private long bytesNeeded;
  private long bytesCached;
  private long filesNeeded;
  private long filesCached;

  public final static class DirectiveList
      extends IntrusiveCollection<CacheDirective> {
    private final CachePool cachePool;

    private DirectiveList(CachePool cachePool) {
      this.cachePool = cachePool;
    }

    public CachePool getCachePool() {
      return cachePool;
    }
  }

  @Nonnull
  private final DirectiveList directiveList = new DirectiveList(this);

  /**
   * Create a new cache pool based on a CachePoolInfo object and the defaults.
   * We will fill in information that was not supplied according to the
   * defaults.
   */
  static CachePool createFromInfoAndDefaults(CachePoolInfo info)
      throws IOException {
    UserGroupInformation ugi = null;
    String ownerName = info.getOwnerName();
    if (ownerName == null) {
      ugi = NameNode.getRemoteUser();
      ownerName = ugi.getShortUserName();
    }
    String groupName = info.getGroupName();
    if (groupName == null) {
      if (ugi == null) {
        ugi = NameNode.getRemoteUser();
      }
      groupName = ugi.getPrimaryGroupName();
    }
    FsPermission mode = (info.getMode() == null) ? 
        FsPermission.getCachePoolDefault() : info.getMode();
    long limit = info.getLimit() == null ?
        CachePoolInfo.DEFAULT_LIMIT : info.getLimit();
    long maxRelativeExpiry = info.getMaxRelativeExpiryMs() == null ?
        CachePoolInfo.DEFAULT_MAX_RELATIVE_EXPIRY :
        info.getMaxRelativeExpiryMs();
    return new CachePool(info.getPoolName(),
        ownerName, groupName, mode, limit, maxRelativeExpiry);
  }

  /**
   * Create a new cache pool based on a CachePoolInfo object.
   * No fields in the CachePoolInfo can be blank.
   */
  static CachePool createFromInfo(CachePoolInfo info) {
    return new CachePool(info.getPoolName(),
        info.getOwnerName(), info.getGroupName(),
        info.getMode(), info.getLimit(), info.getMaxRelativeExpiryMs());
  }

  CachePool(String poolName, String ownerName, String groupName,
      FsPermission mode, long limit, long maxRelativeExpiry) {
    Preconditions.checkNotNull(poolName);
    Preconditions.checkNotNull(ownerName);
    Preconditions.checkNotNull(groupName);
    Preconditions.checkNotNull(mode);
    this.poolName = poolName;
    this.ownerName = ownerName;
    this.groupName = groupName;
    this.mode = new FsPermission(mode);
    this.limit = limit;
    this.maxRelativeExpiryMs = maxRelativeExpiry;
  }

  public String getPoolName() {
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

  public FsPermission getMode() {
    return mode;
  }

  public CachePool setMode(FsPermission mode) {
    this.mode = new FsPermission(mode);
    return this;
  }

  public long getLimit() {
    return limit;
  }

  public CachePool setLimit(long bytes) {
    this.limit = bytes;
    return this;
  }

  public long getMaxRelativeExpiryMs() {
    return maxRelativeExpiryMs;
  }

  public CachePool setMaxRelativeExpiryMs(long expiry) {
    this.maxRelativeExpiryMs = expiry;
    return this;
  }

  /**
   * Get either full or partial information about this CachePool.
   *
   * @param fullInfo
   *          If true, only the name will be returned (i.e., what you 
   *          would get if you didn't have read permission for this pool.)
   * @return
   *          Cache pool information.
   */
  CachePoolInfo getInfo(boolean fullInfo) {
    CachePoolInfo info = new CachePoolInfo(poolName);
    if (!fullInfo) {
      return info;
    }
    return info.setOwnerName(ownerName).
        setGroupName(groupName).
        setMode(new FsPermission(mode)).
        setLimit(limit).
        setMaxRelativeExpiryMs(maxRelativeExpiryMs);
  }

  /**
   * Resets statistics related to this CachePool
   */
  public void resetStatistics() {
    bytesNeeded = 0;
    bytesCached = 0;
    filesNeeded = 0;
    filesCached = 0;
  }

  public void addBytesNeeded(long bytes) {
    bytesNeeded += bytes;
  }

  public void addBytesCached(long bytes) {
    bytesCached += bytes;
  }

  public void addFilesNeeded(long files) {
    filesNeeded += files;
  }

  public void addFilesCached(long files) {
    filesCached += files;
  }

  public long getBytesNeeded() {
    return bytesNeeded;
  }

  public long getBytesCached() {
    return bytesCached;
  }

  public long getBytesOverlimit() {
    return Math.max(bytesNeeded-limit, 0);
  }

  public long getFilesNeeded() {
    return filesNeeded;
  }

  public long getFilesCached() {
    return filesCached;
  }

  /**
   * Get statistics about this CachePool.
   *
   * @return   Cache pool statistics.
   */
  private CachePoolStats getStats() {
    return new CachePoolStats.Builder().
        setBytesNeeded(bytesNeeded).
        setBytesCached(bytesCached).
        setBytesOverlimit(getBytesOverlimit()).
        setFilesNeeded(filesNeeded).
        setFilesCached(filesCached).
        build();
  }

  /**
   * Returns a CachePoolInfo describing this CachePool based on the permissions
   * of the calling user. Unprivileged users will see only minimal descriptive
   * information about the pool.
   * 
   * @param pc Permission checker to be used to validate the user's permissions,
   *          or null
   * @return CachePoolEntry describing this CachePool
   */
  public CachePoolEntry getEntry(FSPermissionChecker pc) {
    boolean hasPermission = true;
    if (pc != null) {
      try {
        pc.checkPermission(this, FsAction.READ);
      } catch (AccessControlException e) {
        hasPermission = false;
      }
    }
    return new CachePoolEntry(getInfo(hasPermission), 
        hasPermission ? getStats() : new CachePoolStats.Builder().build());
  }

  public String toString() {
    return new StringBuilder().
        append("{ ").append("poolName:").append(poolName).
        append(", ownerName:").append(ownerName).
        append(", groupName:").append(groupName).
        append(", mode:").append(mode).
        append(", limit:").append(limit).
        append(", maxRelativeExpiryMs:").append(maxRelativeExpiryMs).
        append(" }").toString();
  }

  public DirectiveList getDirectiveList() {
    return directiveList;
  }
}
