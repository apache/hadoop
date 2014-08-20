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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.DstReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithName;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ChunkedArrayList;
import org.apache.hadoop.hdfs.util.Diff;
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
  final INode setUser(String user, int latestSnapshotId)
      throws QuotaExceededException {
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
  final INode setGroup(String group, int latestSnapshotId)
      throws QuotaExceededException {
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
  INode setPermission(FsPermission permission, int latestSnapshotId) 
      throws QuotaExceededException {
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

  final INode addAclFeature(AclFeature aclFeature, int latestSnapshotId)
      throws QuotaExceededException {
    recordModification(latestSnapshotId);
    addAclFeature(aclFeature);
    return this;
  }

  abstract void removeAclFeature();

  final INode removeAclFeature(int latestSnapshotId)
      throws QuotaExceededException {
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
  
  final INode addXAttrFeature(XAttrFeature xAttrFeature, int latestSnapshotId) 
      throws QuotaExceededException {
    recordModification(latestSnapshotId);
    addXAttrFeature(xAttrFeature);
    return this;
  }
  
  /**
   * Remove <code>XAttrFeature</code> 
   */
  abstract void removeXAttrFeature();
  
  final INode removeXAttrFeature(int lastestSnapshotId)
      throws QuotaExceededException {
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
    if (latestSnapshotId == Snapshot.CURRENT_STATE_ID) {
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
    final INode child = parentDir.getChild(getLocalNameBytes(),
        latestSnapshotId);
    if (this == child) {
      return true;
    }
    if (child == null || !(child.isReference())) {
      return false;
    }
    return this == child.asReference().getReferredINode();
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
  abstract void recordModification(final int latestSnapshotId)
      throws QuotaExceededException;

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
   * @param snapshotId
   *          The id of the snapshot to delete. 
   *          {@link Snapshot#CURRENT_STATE_ID} means to delete the current
   *          file/directory.
   * @param priorSnapshotId
   *          The id of the latest snapshot before the to-be-deleted snapshot.
   *          When deleting a current inode, this parameter captures the latest
   *          snapshot.
   * @param collectedBlocks
   *          blocks collected from the descents for further block
   *          deletion/update will be added to the given map.
   * @param removedINodes
   *          INodes collected from the descents for further cleaning up of 
   *          inodeMap
   * @return quota usage delta when deleting a snapshot
   */
  public abstract Quota.Counts cleanSubtree(final int snapshotId,
      int priorSnapshotId, BlocksMapUpdateInfo collectedBlocks,
      List<INode> removedINodes, boolean countDiffChange)
      throws QuotaExceededException;
  
  /**
   * Destroy self and clear everything! If the INode is a file, this method
   * collects its blocks for further block deletion. If the INode is a
   * directory, the method goes down the subtree and collects blocks from the
   * descents, and clears its parent/children references as well. The method
   * also clears the diff list if the INode contains snapshot diff list.
   * 
   * @param collectedBlocks
   *          blocks collected from the descents for further block
   *          deletion/update will be added to this map.
   * @param removedINodes
   *          INodes collected from the descents for further cleaning up of
   *          inodeMap
   */
  public abstract void destroyAndCollectBlocks(
      BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes);

  /** Compute {@link ContentSummary}. Blocking call */
  public final ContentSummary computeContentSummary() {
    return computeAndConvertContentSummary(
        new ContentSummaryComputationContext());
  }

  /**
   * Compute {@link ContentSummary}. 
   */
  public final ContentSummary computeAndConvertContentSummary(
      ContentSummaryComputationContext summary) {
    Content.Counts counts = computeContentSummary(summary).getCounts();
    final Quota.Counts q = getQuotaCounts();
    return new ContentSummary(counts.get(Content.LENGTH),
        counts.get(Content.FILE) + counts.get(Content.SYMLINK),
        counts.get(Content.DIRECTORY), q.get(Quota.NAMESPACE),
        counts.get(Content.DISKSPACE), q.get(Quota.DISKSPACE));
  }

  /**
   * Count subtree content summary with a {@link Content.Counts}.
   *
   * @param summary the context object holding counts for the subtree.
   * @return The same objects as summary.
   */
  public abstract ContentSummaryComputationContext computeContentSummary(
      ContentSummaryComputationContext summary);

  
  /**
   * Check and add namespace/diskspace consumed to itself and the ancestors.
   * @throws QuotaExceededException if quote is violated.
   */
  public void addSpaceConsumed(long nsDelta, long dsDelta, boolean verify) 
      throws QuotaExceededException {
    addSpaceConsumed2Parent(nsDelta, dsDelta, verify);
  }

  /**
   * Check and add namespace/diskspace consumed to itself and the ancestors.
   * @throws QuotaExceededException if quote is violated.
   */
  void addSpaceConsumed2Parent(long nsDelta, long dsDelta, boolean verify) 
      throws QuotaExceededException {
    if (parent != null) {
      parent.addSpaceConsumed(nsDelta, dsDelta, verify);
    }
  }

  /**
   * Get the quota set for this inode
   * @return the quota counts.  The count is -1 if it is not set.
   */
  public Quota.Counts getQuotaCounts() {
    return Quota.Counts.newInstance(-1, -1);
  }
  
  public final boolean isQuotaSet() {
    final Quota.Counts q = getQuotaCounts();
    return q.get(Quota.NAMESPACE) >= 0 || q.get(Quota.DISKSPACE) >= 0;
  }
  
  /**
   * Count subtree {@link Quota#NAMESPACE} and {@link Quota#DISKSPACE} usages.
   */
  public final Quota.Counts computeQuotaUsage() {
    return computeQuotaUsage(new Quota.Counts(), true);
  }

  /**
   * Count subtree {@link Quota#NAMESPACE} and {@link Quota#DISKSPACE} usages.
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
   * @param counts The subtree counts for returning.
   * @param useCache Whether to use cached quota usage. Note that 
   *                 {@link WithName} node never uses cache for its subtree.
   * @param lastSnapshotId {@link Snapshot#CURRENT_STATE_ID} indicates the 
   *                       computation is in the current tree. Otherwise the id
   *                       indicates the computation range for a 
   *                       {@link WithName} node.
   * @return The same objects as the counts parameter.
   */
  public abstract Quota.Counts computeQuotaUsage(Quota.Counts counts,
      boolean useCache, int lastSnapshotId);

  public final Quota.Counts computeQuotaUsage(Quota.Counts counts,
      boolean useCache) {
    return computeQuotaUsage(counts, useCache, Snapshot.CURRENT_STATE_ID);
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
    return FSDirectory.getFullPathName(this);
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
  public abstract INode updateModificationTime(long mtime, int latestSnapshotId) 
      throws QuotaExceededException;

  /** Set the last modification time of inode. */
  public abstract void setModificationTime(long modificationTime);

  /** Set the last modification time of inode. */
  public final INode setModificationTime(long modificationTime,
      int latestSnapshotId) throws QuotaExceededException {
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
  public final INode setAccessTime(long accessTime, int latestSnapshotId)
      throws QuotaExceededException {
    recordModification(latestSnapshotId);
    setAccessTime(accessTime);
    return this;
  }


  /**
   * Breaks {@code path} into components.
   * @return array of byte arrays each of which represents
   * a single path component.
   */
  @VisibleForTesting
  public static byte[][] getPathComponents(String path) {
    return getPathComponents(getPathNames(path));
  }

  /** Convert strings to byte arrays for path components. */
  static byte[][] getPathComponents(String[] strings) {
    if (strings.length == 0) {
      return new byte[][]{null};
    }
    byte[][] bytes = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++)
      bytes[i] = DFSUtil.string2Bytes(strings[i]);
    return bytes;
  }

  /**
   * Splits an absolute {@code path} into an array of path components.
   * @throws AssertionError if the given path is invalid.
   * @return array of path components.
   */
  static String[] getPathNames(String path) {
    if (path == null || !path.startsWith(Path.SEPARATOR)) {
      throw new AssertionError("Absolute path required");
    }
    return StringUtils.split(path, Path.SEPARATOR_CHAR);
  }

  @Override
  public final int compareTo(byte[] bytes) {
    return DFSUtil.compareBytes(getLocalNameBytes(), bytes);
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
    dumpTreeRecursively(new PrintWriter(out, true), new StringBuilder(),
        Snapshot.CURRENT_STATE_ID);
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
   * Information used for updating the blocksMap when deleting files.
   */
  public static class BlocksMapUpdateInfo {
    /**
     * The list of blocks that need to be removed from blocksMap
     */
    private final List<Block> toDeleteList;
    
    public BlocksMapUpdateInfo() {
      toDeleteList = new ChunkedArrayList<Block>();
    }
    
    /**
     * @return The list of blocks that need to be removed from blocksMap
     */
    public List<Block> getToDeleteList() {
      return toDeleteList;
    }
    
    /**
     * Add a to-be-deleted block into the
     * {@link BlocksMapUpdateInfo#toDeleteList}
     * @param toDelete the to-be-deleted block
     */
    public void addDeleteBlock(Block toDelete) {
      if (toDelete != null) {
        toDeleteList.add(toDelete);
      }
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
