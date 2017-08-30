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

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.DstReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithName;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.Diff;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ChunkedArrayList;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * We keep an in-memory representation of the file/block hierarchy.
 * This is a base INode class containing common fields for file and 
 * directory inodes.
 */
@InterfaceAudience.Private
public abstract class INode implements INodeAttributes, Diff.Element<byte[]> {
  public static final Log LOG = LogFactory.getLog(INode.class);

  /** parent is either an {@link INodeDirectory} or an {@link INodeReference}.*/
  private INode parent = null;

  INode(INode parent) {
    this.parent = parent;
  }

  /** Get inode id */
  public abstract long getId();

  /**
   * Check whether this is the root inode.
   */
  final boolean isRoot() {
    return getLocalNameBytes().length == 0;
  }

  /** Get the {@link PermissionStatus} */
  abstract PermissionStatus getPermissionStatus(int snapshotId);

  /** The same as getPermissionStatus(null). */
  final PermissionStatus getPermissionStatus() {
    return getPermissionStatus(Snapshot.CURRENT_STATE_ID);
  }

  /**
   * @param snapshotId
   *          if it is not {@link Snapshot#CURRENT_STATE_ID}, get the result
   *          from the given snapshot; otherwise, get the result from the
   *          current inode.
   * @return user name
   */
  abstract String getUserName(int snapshotId);

  /** The same as getUserName(Snapshot.CURRENT_STATE_ID). */
  @Override
  public final String getUserName() {
    return getUserName(Snapshot.CURRENT_STATE_ID);
  }

  /** Set user */
  abstract void setUser(String user);

  /** Set user */
  final INode setUser(String user, int latestSnapshotId) {
    recordModification(latestSnapshotId);
    setUser(user);
    return this;
  }
  /**
   * @param snapshotId
   *          if it is not {@link Snapshot#CURRENT_STATE_ID}, get the result
   *          from the given snapshot; otherwise, get the result from the
   *          current inode.
   * @return group name
   */
  abstract String getGroupName(int snapshotId);

  /** The same as getGroupName(Snapshot.CURRENT_STATE_ID). */
  @Override
  public final String getGroupName() {
    return getGroupName(Snapshot.CURRENT_STATE_ID);
  }

  /** Set group */
  abstract void setGroup(String group);

  /** Set group */
  final INode setGroup(String group, int latestSnapshotId) {
    recordModification(latestSnapshotId);
    setGroup(group);
    return this;
  }

  /**
   * @param snapshotId
   *          if it is not {@link Snapshot#CURRENT_STATE_ID}, get the result
   *          from the given snapshot; otherwise, get the result from the
   *          current inode.
   * @return permission.
   */
  abstract FsPermission getFsPermission(int snapshotId);
  
  /** The same as getFsPermission(Snapshot.CURRENT_STATE_ID). */
  @Override
  public final FsPermission getFsPermission() {
    return getFsPermission(Snapshot.CURRENT_STATE_ID);
  }

  /** Set the {@link FsPermission} of this {@link INode} */
  abstract void setPermission(FsPermission permission);

  /** Set the {@link FsPermission} of this {@link INode} */
  INode setPermission(FsPermission permission, int latestSnapshotId) {
    recordModification(latestSnapshotId);
    setPermission(permission);
    return this;
  }

  abstract AclFeature getAclFeature(int snapshotId);

  @Override
  public final AclFeature getAclFeature() {
    return getAclFeature(Snapshot.CURRENT_STATE_ID);
  }

  abstract void addAclFeature(AclFeature aclFeature);

  final INode addAclFeature(AclFeature aclFeature, int latestSnapshotId) {
    recordModification(latestSnapshotId);
    addAclFeature(aclFeature);
    return this;
  }

  abstract void removeAclFeature();

  final INode removeAclFeature(int latestSnapshotId) {
    recordModification(latestSnapshotId);
    removeAclFeature();
    return this;
  }

  /**
   * @param snapshotId
   *          if it is not {@link Snapshot#CURRENT_STATE_ID}, get the result
   *          from the given snapshot; otherwise, get the result from the
   *          current inode.
   * @return XAttrFeature
   */  
  abstract XAttrFeature getXAttrFeature(int snapshotId);
  
  @Override
  public final XAttrFeature getXAttrFeature() {
    return getXAttrFeature(Snapshot.CURRENT_STATE_ID);
  }
  
  /**
   * Set <code>XAttrFeature</code> 
   */
  abstract void addXAttrFeature(XAttrFeature xAttrFeature);
  
  final INode addXAttrFeature(XAttrFeature xAttrFeature, int latestSnapshotId) {
    recordModification(latestSnapshotId);
    addXAttrFeature(xAttrFeature);
    return this;
  }
  
  /**
   * Remove <code>XAttrFeature</code> 
   */
  abstract void removeXAttrFeature();
  
  final INode removeXAttrFeature(int lastestSnapshotId) {
    recordModification(lastestSnapshotId);
    removeXAttrFeature();
    return this;
  }
  
  /**
   * @return if the given snapshot id is {@link Snapshot#CURRENT_STATE_ID},
   *         return this; otherwise return the corresponding snapshot inode.
   */
  public INodeAttributes getSnapshotINode(final int snapshotId) {
    return this;
  }

  /** Is this inode in the latest snapshot? */
  public final boolean isInLatestSnapshot(final int latestSnapshotId) {
    if (latestSnapshotId == Snapshot.CURRENT_STATE_ID ||
        latestSnapshotId == Snapshot.NO_SNAPSHOT_ID) {
      return false;
    }
    // if parent is a reference node, parent must be a renamed node. We can 
    // stop the check at the reference node.
    if (parent != null && parent.isReference()) {
      return true;
    }
    final INodeDirectory parentDir = getParent();
    if (parentDir == null) { // root
      return true;
    }
    if (!parentDir.isInLatestSnapshot(latestSnapshotId)) {
      return false;
    }
    final INode child = parentDir.getChild(getLocalNameBytes(), latestSnapshotId);
    if (this == child) {
      return true;
    }
    return child != null && child.isReference() &&
        this == child.asReference().getReferredINode();
  }
  
  /** @return true if the given inode is an ancestor directory of this inode. */
  public final boolean isAncestorDirectory(final INodeDirectory dir) {
    for(INodeDirectory p = getParent(); p != null; p = p.getParent()) {
      if (p == dir) {
        return true;
      }
    }
    return false;
  }

  /**
   * When {@link #recordModification} is called on a referred node,
   * this method tells which snapshot the modification should be
   * associated with: the snapshot that belongs to the SRC tree of the rename
   * operation, or the snapshot belonging to the DST tree.
   * 
   * @param latestInDst
   *          id of the latest snapshot in the DST tree above the reference node
   * @return True: the modification should be recorded in the snapshot that
   *         belongs to the SRC tree. False: the modification should be
   *         recorded in the snapshot that belongs to the DST tree.
   */
  public final boolean shouldRecordInSrcSnapshot(final int latestInDst) {
    Preconditions.checkState(!isReference());

    if (latestInDst == Snapshot.CURRENT_STATE_ID) {
      return true;
    }
    INodeReference withCount = getParentReference();
    if (withCount != null) {
      int dstSnapshotId = withCount.getParentReference().getDstSnapshotId();
      if (dstSnapshotId != Snapshot.CURRENT_STATE_ID
          && dstSnapshotId >= latestInDst) {
        return true;
      }
    }
    return false;
  }

  /**
   * This inode is being modified.  The previous version of the inode needs to
   * be recorded in the latest snapshot.
   *
   * @param latestSnapshotId The id of the latest snapshot that has been taken.
   *                         Note that it is {@link Snapshot#CURRENT_STATE_ID} 
   *                         if no snapshots have been taken.
   */
  abstract void recordModification(final int latestSnapshotId);

  /** Check whether it's a reference. */
  public boolean isReference() {
    return false;
  }

  /** Cast this inode to an {@link INodeReference}.  */
  public INodeReference asReference() {
    throw new IllegalStateException("Current inode is not a reference: "
        + this.toDetailString());
  }

  /**
   * Check whether it's a file.
   */
  public boolean isFile() {
    return false;
  }

  /** Cast this inode to an {@link INodeFile}.  */
  public INodeFile asFile() {
    throw new IllegalStateException("Current inode is not a file: "
        + this.toDetailString());
  }

  /**
   * Check whether it's a directory
   */
  public boolean isDirectory() {
    return false;
  }

  /** Cast this inode to an {@link INodeDirectory}.  */
  public INodeDirectory asDirectory() {
    throw new IllegalStateException("Current inode is not a directory: "
        + this.toDetailString());
  }

  /**
   * Check whether it's a symlink
   */
  public boolean isSymlink() {
    return false;
  }

  /** Cast this inode to an {@link INodeSymlink}.  */
  public INodeSymlink asSymlink() {
    throw new IllegalStateException("Current inode is not a symlink: "
        + this.toDetailString());
  }

  /**
   * Clean the subtree under this inode and collect the blocks from the descents
   * for further block deletion/update. The current inode can either resides in
   * the current tree or be stored as a snapshot copy.
   * 
   * <pre>
   * In general, we have the following rules. 
   * 1. When deleting a file/directory in the current tree, we have different 
   * actions according to the type of the node to delete. 
   * 
   * 1.1 The current inode (this) is an {@link INodeFile}. 
   * 1.1.1 If {@code prior} is null, there is no snapshot taken on ancestors 
   * before. Thus we simply destroy (i.e., to delete completely, no need to save 
   * snapshot copy) the current INode and collect its blocks for further 
   * cleansing.
   * 1.1.2 Else do nothing since the current INode will be stored as a snapshot
   * copy.
   * 
   * 1.2 The current inode is an {@link INodeDirectory}.
   * 1.2.1 If {@code prior} is null, there is no snapshot taken on ancestors 
   * before. Similarly, we destroy the whole subtree and collect blocks.
   * 1.2.2 Else do nothing with the current INode. Recursively clean its 
   * children.
   * 
   * 1.3 The current inode is a file with snapshot.
   * Call recordModification(..) to capture the current states.
   * Mark the INode as deleted.
   * 
   * 1.4 The current inode is an {@link INodeDirectory} with snapshot feature.
   * Call recordModification(..) to capture the current states. 
   * Destroy files/directories created after the latest snapshot 
   * (i.e., the inodes stored in the created list of the latest snapshot).
   * Recursively clean remaining children. 
   *
   * 2. When deleting a snapshot.
   * 2.1 To clean {@link INodeFile}: do nothing.
   * 2.2 To clean {@link INodeDirectory}: recursively clean its children.
   * 2.3 To clean INodeFile with snapshot: delete the corresponding snapshot in
   * its diff list.
   * 2.4 To clean {@link INodeDirectory} with snapshot: delete the corresponding 
   * snapshot in its diff list. Recursively clean its children.
   * </pre>
   *
   * @param reclaimContext
   *        Record blocks and inodes that need to be reclaimed.
   * @param snapshotId
   *        The id of the snapshot to delete.
   *        {@link Snapshot#CURRENT_STATE_ID} means to delete the current
   *        file/directory.
   * @param priorSnapshotId
   *        The id of the latest snapshot before the to-be-deleted snapshot.
   *        When deleting a current inode, this parameter captures the latest
   *        snapshot.
   */
  public abstract void cleanSubtree(ReclaimContext reclaimContext,
      final int snapshotId, int priorSnapshotId);

  /**
   * Destroy self and clear everything! If the INode is a file, this method
   * collects its blocks for further block deletion. If the INode is a
   * directory, the method goes down the subtree and collects blocks from the
   * descents, and clears its parent/children references as well. The method
   * also clears the diff list if the INode contains snapshot diff list.
   *
   * @param reclaimContext
   *        Record blocks and inodes that need to be reclaimed.
   */
  public abstract void destroyAndCollectBlocks(ReclaimContext reclaimContext);

  /** Compute {@link ContentSummary}. Blocking call */
  public final ContentSummary computeContentSummary(
      BlockStoragePolicySuite bsps) throws AccessControlException {
    return computeAndConvertContentSummary(Snapshot.CURRENT_STATE_ID,
        new ContentSummaryComputationContext(bsps));
  }

  /**
   * Compute {@link ContentSummary}. 
   */
  public final ContentSummary computeAndConvertContentSummary(int snapshotId,
      ContentSummaryComputationContext summary) throws AccessControlException {
    computeContentSummary(snapshotId, summary);
    final ContentCounts counts = summary.getCounts();
    final ContentCounts snapshotCounts = summary.getSnapshotCounts();
    final QuotaCounts q = getQuotaCounts();
    return new ContentSummary.Builder().
        length(counts.getLength()).
        fileCount(counts.getFileCount() + counts.getSymlinkCount()).
        directoryCount(counts.getDirectoryCount()).
        quota(q.getNameSpace()).
        spaceConsumed(counts.getStoragespace()).
        spaceQuota(q.getStorageSpace()).
        typeConsumed(counts.getTypeSpaces()).
        typeQuota(q.getTypeSpaces().asArray()).
        snapshotLength(snapshotCounts.getLength()).
        snapshotFileCount(snapshotCounts.getFileCount()).
        snapshotDirectoryCount(snapshotCounts.getDirectoryCount()).
        snapshotSpaceConsumed(snapshotCounts.getStoragespace()).
        erasureCodingPolicy(summary.getErasureCodingPolicyName(this)).
        build();
  }

  /**
   * Count subtree content summary with a {@link ContentCounts}.
   *
   * @param snapshotId Specify the time range for the calculation. If this
   *                   parameter equals to {@link Snapshot#CURRENT_STATE_ID},
   *                   the result covers both the current states and all the
   *                   snapshots. Otherwise the result only covers all the
   *                   files/directories contained in the specific snapshot.
   * @param summary the context object holding counts for the subtree.
   * @return The same objects as summary.
   */
  public abstract ContentSummaryComputationContext computeContentSummary(
      int snapshotId, ContentSummaryComputationContext summary)
      throws AccessControlException;


  /**
   * Check and add namespace/storagespace/storagetype consumed to itself and the ancestors.
   * @throws QuotaExceededException if quote is violated.
   */
  public void addSpaceConsumed(QuotaCounts counts, boolean verify)
    throws QuotaExceededException {
    addSpaceConsumed2Parent(counts, verify);
  }

  /**
   * Check and add namespace/storagespace/storagetype consumed to itself and the ancestors.
   * @throws QuotaExceededException if quote is violated.
   */
  void addSpaceConsumed2Parent(QuotaCounts counts, boolean verify)
    throws QuotaExceededException {
    if (parent != null) {
      parent.addSpaceConsumed(counts, verify);
    }
  }

  /**
   * Get the quota set for this inode
   * @return the quota counts.  The count is -1 if it is not set.
   */
  public QuotaCounts getQuotaCounts() {
    return new QuotaCounts.Builder().
        nameSpace(HdfsConstants.QUOTA_RESET).
        storageSpace(HdfsConstants.QUOTA_RESET).
        typeSpaces(HdfsConstants.QUOTA_RESET).
        build();
  }

  public final boolean isQuotaSet() {
    final QuotaCounts qc = getQuotaCounts();
    return qc.anyNsSsCountGreaterOrEqual(0) || qc.anyTypeSpaceCountGreaterOrEqual(0);
  }

  /**
   * Count subtree {@link Quota#NAMESPACE} and {@link Quota#STORAGESPACE} usages.
   * Entry point for FSDirectory where blockStoragePolicyId is given its initial
   * value.
   */
  public final QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps) {
    final byte storagePolicyId = isSymlink() ?
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED : getStoragePolicyID();
    return computeQuotaUsage(bsps, storagePolicyId, true,
        Snapshot.CURRENT_STATE_ID);
  }

  /**
   * Count subtree {@link Quota#NAMESPACE} and {@link Quota#STORAGESPACE} usages.
   * 
   * With the existence of {@link INodeReference}, the same inode and its
   * subtree may be referred by multiple {@link WithName} nodes and a
   * {@link DstReference} node. To avoid circles while quota usage computation,
   * we have the following rules:
   * 
   * <pre>
   * 1. For a {@link DstReference} node, since the node must be in the current
   * tree (or has been deleted as the end point of a series of rename 
   * operations), we compute the quota usage of the referred node (and its 
   * subtree) in the regular manner, i.e., including every inode in the current
   * tree and in snapshot copies, as well as the size of diff list.
   * 
   * 2. For a {@link WithName} node, since the node must be in a snapshot, we 
   * only count the quota usage for those nodes that still existed at the 
   * creation time of the snapshot associated with the {@link WithName} node.
   * We do not count in the size of the diff list.  
   * <pre>
   *
   * @param bsps Block storage policy suite to calculate intended storage type usage
   * @param blockStoragePolicyId block storage policy id of the current INode
   * @param useCache Whether to use cached quota usage. Note that 
   *                 {@link WithName} node never uses cache for its subtree.
   * @param lastSnapshotId {@link Snapshot#CURRENT_STATE_ID} indicates the 
   *                       computation is in the current tree. Otherwise the id
   *                       indicates the computation range for a 
   *                       {@link WithName} node.
   * @return The subtree quota counts.
   */
  public abstract QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId);

  public final QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      boolean useCache) {
    final byte storagePolicyId = isSymlink() ?
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED : getStoragePolicyID();
    return computeQuotaUsage(bsps, storagePolicyId, useCache,
        Snapshot.CURRENT_STATE_ID);
  }

  /**
   * @return null if the local name is null; otherwise, return the local name.
   */
  public final String getLocalName() {
    final byte[] name = getLocalNameBytes();
    return name == null? null: DFSUtil.bytes2String(name);
  }

  @Override
  public final byte[] getKey() {
    return getLocalNameBytes();
  }

  /**
   * Set local file name
   */
  public abstract void setLocalName(byte[] name);

  public String getFullPathName() {
    // Get the full path name of this inode.
    if (isRoot()) {
      return Path.SEPARATOR;
    }
    // compute size of needed bytes for the path
    int idx = 0;
    for (INode inode = this; inode != null; inode = inode.getParent()) {
      // add component + delimiter (if not tail component)
      idx += inode.getLocalNameBytes().length + (inode != this ? 1 : 0);
    }
    byte[] path = new byte[idx];
    for (INode inode = this; inode != null; inode = inode.getParent()) {
      if (inode != this) {
        path[--idx] = Path.SEPARATOR_CHAR;
      }
      byte[] name = inode.getLocalNameBytes();
      idx -= name.length;
      System.arraycopy(name, 0, path, idx, name.length);
    }
    return DFSUtil.bytes2String(path);
  }

  public byte[][] getPathComponents() {
    int n = 0;
    for (INode inode = this; inode != null; inode = inode.getParent()) {
      n++;
    }
    byte[][] components = new byte[n][];
    for (INode inode = this; inode != null; inode = inode.getParent()) {
      components[--n] = inode.getLocalNameBytes();
    }
    return components;
  }

  @Override
  public String toString() {
    return getLocalName();
  }

  @VisibleForTesting
  public final String getObjectString() {
    return getClass().getSimpleName() + "@"
        + Integer.toHexString(super.hashCode());
  }

  /** @return a string description of the parent. */
  @VisibleForTesting
  public final String getParentString() {
    final INodeReference parentRef = getParentReference();
    if (parentRef != null) {
      return "parentRef=" + parentRef.getLocalName() + "->";
    } else {
      final INodeDirectory parentDir = getParent();
      if (parentDir != null) {
        return "parentDir=" + parentDir.getLocalName() + "/";
      } else {
        return "parent=null";
      }
    }
  }

  @VisibleForTesting
  public String toDetailString() {
    return toString() + "(" + getObjectString() + "), " + getParentString();
  }

  /** @return the parent directory */
  public final INodeDirectory getParent() {
    return parent == null? null
        : parent.isReference()? getParentReference().getParent(): parent.asDirectory();
  }

  /**
   * @return the parent as a reference if this is a referred inode;
   *         otherwise, return null.
   */
  public INodeReference getParentReference() {
    return parent == null || !parent.isReference()? null: (INodeReference)parent;
  }

  /** Set parent directory */
  public final void setParent(INodeDirectory parent) {
    this.parent = parent;
  }

  /** Set container. */
  public final void setParentReference(INodeReference parent) {
    this.parent = parent;
  }

  /** Clear references to other objects. */
  public void clear() {
    setParent(null);
  }

  /**
   * @param snapshotId
   *          if it is not {@link Snapshot#CURRENT_STATE_ID}, get the result
   *          from the given snapshot; otherwise, get the result from the
   *          current inode.
   * @return modification time.
   */
  abstract long getModificationTime(int snapshotId);

  /** The same as getModificationTime(Snapshot.CURRENT_STATE_ID). */
  @Override
  public final long getModificationTime() {
    return getModificationTime(Snapshot.CURRENT_STATE_ID);
  }

  /** Update modification time if it is larger than the current value. */
  public abstract INode updateModificationTime(long mtime, int latestSnapshotId);

  /** Set the last modification time of inode. */
  public abstract void setModificationTime(long modificationTime);

  /** Set the last modification time of inode. */
  public final INode setModificationTime(long modificationTime,
      int latestSnapshotId) {
    recordModification(latestSnapshotId);
    setModificationTime(modificationTime);
    return this;
  }

  /**
   * @param snapshotId
   *          if it is not {@link Snapshot#CURRENT_STATE_ID}, get the result
   *          from the given snapshot; otherwise, get the result from the
   *          current inode.
   * @return access time
   */
  abstract long getAccessTime(int snapshotId);

  /** The same as getAccessTime(Snapshot.CURRENT_STATE_ID). */
  @Override
  public final long getAccessTime() {
    return getAccessTime(Snapshot.CURRENT_STATE_ID);
  }

  /**
   * Set last access time of inode.
   */
  public abstract void setAccessTime(long accessTime);

  /**
   * Set last access time of inode.
   */
  public final INode setAccessTime(long accessTime, int latestSnapshotId,
      boolean skipCaptureAccessTimeOnlyChangeInSnapshot) {
    if (!skipCaptureAccessTimeOnlyChangeInSnapshot) {
      recordModification(latestSnapshotId);
    }
    setAccessTime(accessTime);
    return this;
  }

  /**
   * @return the latest block storage policy id of the INode. Specifically,
   * if a storage policy is directly specified on the INode then return the ID
   * of that policy. Otherwise follow the latest parental path and return the
   * ID of the first specified storage policy.
   */
  public abstract byte getStoragePolicyID();

  /**
   * @return the storage policy directly specified on the INode. Return
   * {@link HdfsConstants#BLOCK_STORAGE_POLICY_ID_UNSPECIFIED} if no policy has
   * been specified.
   */
  public abstract byte getLocalStoragePolicyID();

  /**
   * Get the storage policy ID while computing quota usage
   * @param parentStoragePolicyId the storage policy ID of the parent directory
   * @return the storage policy ID of this INode. Note that for an
   * {@link INodeSymlink} we return {@link HdfsConstants#BLOCK_STORAGE_POLICY_ID_UNSPECIFIED}
   * instead of throwing Exception
   */
  public byte getStoragePolicyIDForQuota(byte parentStoragePolicyId) {
    byte localId = isSymlink() ?
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED : getLocalStoragePolicyID();
    return localId != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED ?
        localId : parentStoragePolicyId;
  }

  /**
   * Breaks {@code path} into components.
   * @return array of byte arrays each of which represents
   * a single path component.
   */
  @VisibleForTesting
  public static byte[][] getPathComponents(String path) {
    checkAbsolutePath(path);
    return DFSUtil.getPathComponents(path);
  }

  /**
   * Splits an absolute {@code path} into an array of path components.
   * @throws AssertionError if the given path is invalid.
   * @return array of path components.
   */
  public static String[] getPathNames(String path) {
    checkAbsolutePath(path);
    return StringUtils.split(path, Path.SEPARATOR_CHAR);
  }

  /**
   * Verifies if the path informed is a valid absolute path.
   * @param path the absolute path to validate.
   * @return true if the path is valid.
   */
  static boolean isValidAbsolutePath(final String path){
    return path != null && path.startsWith(Path.SEPARATOR);
  }

  private static void checkAbsolutePath(final String path) {
    if (!isValidAbsolutePath(path)) {
      throw new AssertionError("Absolute path required, but got '"
          + path + "'");
    }
  }

  @Override
  public final int compareTo(byte[] bytes) {
    return DFSUtilClient.compareBytes(getLocalNameBytes(), bytes);
  }

  @Override
  public final boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || !(that instanceof INode)) {
      return false;
    }
    return getId() == ((INode) that).getId();
  }

  @Override
  public final int hashCode() {
    long id = getId();
    return (int)(id^(id>>>32));  
  }
  
  /**
   * Dump the subtree starting from this inode.
   * @return a text representation of the tree.
   */
  @VisibleForTesting
  public final StringBuffer dumpTreeRecursively() {
    final StringWriter out = new StringWriter(); 
    dumpTreeRecursively(new PrintWriter(out, true), new StringBuilder(),
        Snapshot.CURRENT_STATE_ID);
    return out.getBuffer();
  }

  @VisibleForTesting
  public final void dumpTreeRecursively(PrintStream out) {
    out.println(dumpTreeRecursively().toString());
  }

  /**
   * Dump tree recursively.
   * @param prefix The prefix string that each line should print.
   */
  @VisibleForTesting
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      int snapshotId) {
    out.print(prefix);
    out.print(" ");
    final String name = getLocalName();
    out.print(name.isEmpty()? "/": name);
    out.print("   (");
    out.print(getObjectString());
    out.print("), ");
    out.print(getParentString());
    out.print(", " + getPermissionStatus(snapshotId));
  }

  /**
   * Information used to record quota usage delta. This data structure is
   * usually passed along with an operation like {@link #cleanSubtree}. Note
   * that after the operation the delta counts should be decremented from the
   * ancestral directories' quota usage.
   */
  public static class QuotaDelta {
    private final QuotaCounts counts;
    /**
     * The main usage of this map is to track the quota delta that should be
     * applied to another path. This usually happens when we reclaim INodes and
     * blocks while deleting snapshots, and hit an INodeReference. Because the
     * quota usage for a renamed+snapshotted file/directory is counted in both
     * the current and historical parents, any change of its quota usage may
     * need to be propagated along its parent paths both before and after the
     * rename.
     */
    private final Map<INode, QuotaCounts> updateMap;

    /**
     * When deleting a snapshot we may need to update the quota for directories
     * with quota feature. This map is used to capture these directories and
     * their quota usage updates.
     */
    private final Map<INodeDirectory, QuotaCounts> quotaDirMap;

    public QuotaDelta() {
      counts = new QuotaCounts.Builder().build();
      updateMap = Maps.newHashMap();
      quotaDirMap = Maps.newHashMap();
    }

    public void add(QuotaCounts update) {
      counts.add(update);
    }

    public void addUpdatePath(INodeReference inode, QuotaCounts update) {
      QuotaCounts c = updateMap.get(inode);
      if (c == null) {
        c = new QuotaCounts.Builder().build();
        updateMap.put(inode, c);
      }
      c.add(update);
    }

    public void addQuotaDirUpdate(INodeDirectory dir, QuotaCounts update) {
      Preconditions.checkState(dir.isQuotaSet());
      QuotaCounts c = quotaDirMap.get(dir);
      if (c == null) {
        quotaDirMap.put(dir, update);
      } else {
        c.add(update);
      }
    }

    public QuotaCounts getCountsCopy() {
      final QuotaCounts copy = new QuotaCounts.Builder().build();
      copy.add(counts);
      return copy;
    }

    public void setCounts(QuotaCounts c) {
      this.counts.setNameSpace(c.getNameSpace());
      this.counts.setStorageSpace(c.getStorageSpace());
      this.counts.setTypeSpaces(c.getTypeSpaces());
    }

    public long getNsDelta() {
      long nsDelta = counts.getNameSpace();
      for (Map.Entry<INode, QuotaCounts> entry : updateMap.entrySet()) {
        nsDelta += entry.getValue().getNameSpace();
      }
      return nsDelta;
    }

    public Map<INode, QuotaCounts> getUpdateMap() {
      return ImmutableMap.copyOf(updateMap);
    }

    public Map<INodeDirectory, QuotaCounts> getQuotaDirMap() {
      return ImmutableMap.copyOf(quotaDirMap);
    }
  }

  /**
   * Context object to record blocks and inodes that need to be reclaimed
   */
  public static class ReclaimContext {
    protected final BlockStoragePolicySuite bsps;
    protected final BlocksMapUpdateInfo collectedBlocks;
    protected final List<INode> removedINodes;
    protected final List<Long> removedUCFiles;
    /** Used to collect quota usage delta */
    private final QuotaDelta quotaDelta;

    /**
     * @param bsps
 *          block storage policy suite to calculate intended storage type
 *          usage
     * @param collectedBlocks
*          blocks collected from the descents for further block
*          deletion/update will be added to the given map.
     * @param removedINodes
*          INodes collected from the descents for further cleaning up of
     * @param removedUCFiles
     */
    public ReclaimContext(
        BlockStoragePolicySuite bsps, BlocksMapUpdateInfo collectedBlocks,
        List<INode> removedINodes, List<Long> removedUCFiles) {
      this.bsps = bsps;
      this.collectedBlocks = collectedBlocks;
      this.removedINodes = removedINodes;
      this.removedUCFiles = removedUCFiles;
      this.quotaDelta = new QuotaDelta();
    }

    public BlockStoragePolicySuite storagePolicySuite() {
      return bsps;
    }

    public BlocksMapUpdateInfo collectedBlocks() {
      return collectedBlocks;
    }

    public QuotaDelta quotaDelta() {
      return quotaDelta;
    }

    /**
     * make a copy with the same collectedBlocks, removedINodes, and
     * removedUCFiles but a new quotaDelta.
     */
    public ReclaimContext getCopy() {
      return new ReclaimContext(bsps, collectedBlocks, removedINodes,
          removedUCFiles);
    }
  }

  /**
   * Information used for updating the blocksMap when deleting files.
   */
  public static class BlocksMapUpdateInfo {
    /**
     * The blocks whose replication factor need to be updated.
     */
    public static class UpdatedReplicationInfo {
      /**
       * the expected replication after the update.
       */
      private final short targetReplication;
      /**
       * The block whose replication needs to be updated.
       */
      private final BlockInfo block;

      public UpdatedReplicationInfo(short targetReplication, BlockInfo block) {
        this.targetReplication = targetReplication;
        this.block = block;
      }

      public BlockInfo block() {
        return block;
      }

      public short targetReplication() {
        return targetReplication;
      }
    }
    /**
     * The list of blocks that need to be removed from blocksMap
     */
    private final List<BlockInfo> toDeleteList;
    /**
     * The list of blocks whose replication factor needs to be adjusted
     */
    private final List<UpdatedReplicationInfo> toUpdateReplicationInfo;

    public BlocksMapUpdateInfo() {
      toDeleteList = new ChunkedArrayList<>();
      toUpdateReplicationInfo = new ChunkedArrayList<>();
    }
    
    /**
     * @return The list of blocks that need to be removed from blocksMap
     */
    public List<BlockInfo> getToDeleteList() {
      return toDeleteList;
    }

    public List<UpdatedReplicationInfo> toUpdateReplicationInfo() {
      return toUpdateReplicationInfo;
    }

    /**
     * Add a to-be-deleted block into the
     * {@link BlocksMapUpdateInfo#toDeleteList}
     * @param toDelete the to-be-deleted block
     */
    public void addDeleteBlock(BlockInfo toDelete) {
      assert toDelete != null : "toDelete is null";
      toDelete.delete();
      toDeleteList.add(toDelete);
    }

    public void addUpdateReplicationFactor(BlockInfo block, short targetRepl) {
      toUpdateReplicationInfo.add(
          new UpdatedReplicationInfo(targetRepl, block));
    }
    /**
     * Clear {@link BlocksMapUpdateInfo#toDeleteList}
     */
    public void clear() {
      toDeleteList.clear();
    }
  }

  /** 
   * INode feature such as {@link FileUnderConstructionFeature}
   * and {@link DirectoryWithQuotaFeature}.
   */
  public interface Feature {
  }
}
