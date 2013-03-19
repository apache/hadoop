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

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

/**
 * An anonymous reference to an inode.
 *
 * This class and its subclasses are used to support multiple access paths.
 * A file/directory may have multiple access paths when it is stored in some
 * snapshots and it is renamed/moved to other locations.
 * 
 * For example,
 * (1) Support we have /abc/foo, say the inode of foo is inode(id=1000,name=foo)
 * (2) create snapshot s0 for /abc
 * (3) mv /abc/foo /xyz/bar, i.e. inode(id=1000,name=...) is renamed from "foo"
 *     to "bar" and its parent becomes /xyz.
 * 
 * Then, /xyz/bar and /abc/.snapshot/s0/foo are two different access paths to
 * the same inode, inode(id=1000,name=bar).
 *
 * With references, we have the following
 * - /abc has a child ref(id=1001,name=foo).
 * - /xyz has a child ref(id=1002) 
 * - Both ref(id=1001,name=foo) and ref(id=1002) point to another reference,
 *   ref(id=1003,count=2).
 * - Finally, ref(id=1003,count=2) points to inode(id=1000,name=bar).
 * 
 * Note 1: For a reference without name, e.g. ref(id=1002), it uses the name
 *         of the referred inode.
 * Note 2: getParent() always returns the parent in the current state, e.g.
 *         inode(id=1000,name=bar).getParent() returns /xyz but not /abc.
 */
public class INodeReference extends INode {
  /**
   * Try to remove the given reference and then return the reference count.
   * If the given inode is not a reference, return -1;
   */
  public static int tryRemoveReference(INode inode) {
    if (!inode.isReference()) {
      return -1;
    }
    return removeReference(inode.asReference());
  }

  /**
   * Remove the given reference and then return the reference count.
   * If the referred inode is not a WithCount, return -1;
   */
  private static int removeReference(INodeReference ref) {
    final INode referred = ref.getReferredINode();
    if (!(referred instanceof WithCount)) {
      return -1;
    }
    return ((WithCount)referred).decrementReferenceCount();
  }

  private INode referred;
  
  INodeReference(INode referred) {
    super(referred.getParent());
    this.referred = referred;

    referred.setParentReference(this);
  }


  public final INode getReferredINode() {
    return referred;
  }

  public final void setReferredINode(INode referred) {
    this.referred = referred;
  }
  
  @Override
  public final boolean isReference() {
    return true;
  }
  
  @Override
  public final INodeReference asReference() {
    return this;
  }

  @Override
  public final boolean isFile() {
    return referred.isFile();
  }
  
  @Override
  public final INodeFile asFile() {
    return referred.asFile();
  }
  
  @Override
  public final boolean isDirectory() {
    return referred.isDirectory();
  }
  
  @Override
  public final INodeDirectory asDirectory() {
    return referred.asDirectory();
  }
  
  @Override
  public final boolean isSymlink() {
    return referred.isSymlink();
  }
  
  @Override
  public final INodeSymlink asSymlink() {
    return referred.asSymlink();
  }

  @Override
  public byte[] getLocalNameBytes() {
    return referred.getLocalNameBytes();
  }

  @Override
  public void setLocalName(byte[] name) {
    referred.setLocalName(name);
  }

  @Override
  public final long getId() {
    return referred.getId();
  }
  
  @Override
  public final PermissionStatus getPermissionStatus(Snapshot snapshot) {
    return referred.getPermissionStatus(snapshot);
  }
  
  @Override
  public final String getUserName(Snapshot snapshot) {
    return referred.getUserName(snapshot);
  }
  
  @Override
  final void setUser(String user) {
    referred.setUser(user);
  }
  
  @Override
  public final String getGroupName(Snapshot snapshot) {
    return referred.getGroupName(snapshot);
  }
  
  @Override
  final void setGroup(String group) {
    referred.setGroup(group);
  }
  
  @Override
  public final FsPermission getFsPermission(Snapshot snapshot) {
    return referred.getFsPermission(snapshot);
  }
  
  @Override
  void setPermission(FsPermission permission) {
    referred.setPermission(permission);
  }
  
  @Override
  public final long getModificationTime(Snapshot snapshot) {
    return referred.getModificationTime(snapshot);
  }
  
  @Override
  public final INode updateModificationTime(long mtime, Snapshot latest)
      throws QuotaExceededException {
    return referred.updateModificationTime(mtime, latest);
  }
  
  @Override
  public final void setModificationTime(long modificationTime) {
    referred.setModificationTime(modificationTime);
  }
  
  @Override
  public final long getAccessTime(Snapshot snapshot) {
    return referred.getAccessTime(snapshot);
  }
  
  @Override
  public final void setAccessTime(long accessTime) {
    referred.setAccessTime(accessTime);
  }

  @Override
  final INode recordModification(Snapshot latest) throws QuotaExceededException {
    return referred.recordModification(latest);
  }

  @Override
  public final Quota.Counts cleanSubtree(Snapshot snapshot, Snapshot prior,
      BlocksMapUpdateInfo collectedBlocks) throws QuotaExceededException {
    return referred.cleanSubtree(snapshot, prior, collectedBlocks);
  }

  @Override
  public final void destroyAndCollectBlocks(BlocksMapUpdateInfo collectedBlocks) {
    if (removeReference(this) <= 0) {
      referred.destroyAndCollectBlocks(collectedBlocks);
    }
  }

  @Override
  public final Content.CountsMap computeContentSummary(Content.CountsMap countsMap) {
    return referred.computeContentSummary(countsMap);
  }

  @Override
  public final Content.Counts computeContentSummary(Content.Counts counts) {
    return referred.computeContentSummary(counts);
  }

  @Override
  public final Quota.Counts computeQuotaUsage(Quota.Counts counts, boolean useCache) {
    return referred.computeQuotaUsage(counts, useCache);
  }
  
  @Override
  public final INode getSnapshotINode(Snapshot snapshot) {
    return referred.getSnapshotINode(snapshot);
  }

  @Override
  public final void addSpaceConsumed(long nsDelta, long dsDelta
      ) throws QuotaExceededException {
    referred.addSpaceConsumed(nsDelta, dsDelta);
  }

  @Override
  public final long getNsQuota() {
    return referred.getNsQuota();
  }

  @Override
  public final long getDsQuota() {
    return referred.getDsQuota();
  }
  
  @Override
  public final void clear() {
    super.clear();
    referred.clear();
    referred = null;
  }

  /** An anonymous reference with reference count. */
  public static class WithCount extends INodeReference {
    private int referenceCount = 0;
    
    WithCount(INode inode) {
      super(inode);
    }
    
    public int incrementReferenceCount() {
      return ++referenceCount;
    }

    public int decrementReferenceCount() {
      return --referenceCount;
    }
  }

  /** A reference with a fixed name. */
  public static class WithName extends INodeReference {

    private final byte[] name;

    public WithName(WithCount referred, byte[] name) {
      super(referred);
      this.name = name;
    }

    @Override
    public final byte[] getLocalNameBytes() {
      return name;
    }

    @Override
    public final void setLocalName(byte[] name) {
      throw new UnsupportedOperationException("Cannot set name: " + getClass()
          + " is immutable.");
    }
  }
}