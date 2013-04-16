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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.server.namenode.Content;
import org.apache.hadoop.hdfs.server.namenode.Content.CountsMap.Key;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;
import org.apache.hadoop.hdfs.util.Diff;
import org.apache.hadoop.hdfs.util.Diff.Container;
import org.apache.hadoop.hdfs.util.Diff.ListType;
import org.apache.hadoop.hdfs.util.Diff.UndoInfo;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import com.google.common.base.Preconditions;

/**
 * The directory with snapshots. It maintains a list of snapshot diffs for
 * storing snapshot data. When there are modifications to the directory, the old
 * data is stored in the latest snapshot, if there is any.
 */
public class INodeDirectoryWithSnapshot extends INodeDirectoryWithQuota {
  /**
   * The difference between the current state and a previous snapshot
   * of the children list of an INodeDirectory.
   */
  static class ChildrenDiff extends Diff<byte[], INode> {
    ChildrenDiff() {}
    
    private ChildrenDiff(final List<INode> created, final List<INode> deleted) {
      super(created, deleted);
    }

    /**
     * Replace the given child from the created/deleted list.
     * @return true if the child is replaced; false if the child is not found.
     */
    private final boolean replace(final ListType type,
        final INode oldChild, final INode newChild) {
      final List<INode> list = getList(type); 
      final int i = search(list, oldChild.getLocalNameBytes());
      if (i < 0) {
        return false;
      }

      final INode removed = list.set(i, newChild);
      Preconditions.checkState(removed == oldChild);
      return true;
    }

    private final boolean removeChild(ListType type, final INode child) {
      final List<INode> list = getList(type);
      final int i = searchIndex(type, child.getLocalNameBytes());
      if (i >= 0 && list.get(i) == child) {
        list.remove(i);
        return true;
      }
      return false;
    }
    
    /** clear the created list */
    private Quota.Counts destroyCreatedList(
        final INodeDirectoryWithSnapshot currentINode,
        final BlocksMapUpdateInfo collectedBlocks) {
      Quota.Counts counts = Quota.Counts.newInstance();
      final List<INode> createdList = getList(ListType.CREATED);
      for (INode c : createdList) {
        c.computeQuotaUsage(counts, true);
        c.destroyAndCollectBlocks(collectedBlocks);
        // c should be contained in the children list, remove it
        currentINode.removeChild(c);
      }
      createdList.clear();
      return counts;
    }
    
    /** clear the deleted list */
    private Quota.Counts destroyDeletedList(
        final BlocksMapUpdateInfo collectedBlocks, 
        final List<INodeReference> refNodes) {
      Quota.Counts counts = Quota.Counts.newInstance();
      final List<INode> deletedList = getList(ListType.DELETED);
      for (INode d : deletedList) {
        if (INodeReference.tryRemoveReference(d) <= 0) {
          d.computeQuotaUsage(counts, false);
          d.destroyAndCollectBlocks(collectedBlocks);
        } else {
          refNodes.add(d.asReference());
        }
      }
      deletedList.clear();
      return counts;
    }
    
    /** Serialize {@link #created} */
    private void writeCreated(DataOutput out) throws IOException {
      final List<INode> created = getList(ListType.CREATED);
      out.writeInt(created.size());
      for (INode node : created) {
        // For INode in created list, we only need to record its local name 
        byte[] name = node.getLocalNameBytes();
        out.writeShort(name.length);
        out.write(name);
      }
    }
    
    /** Serialize {@link #deleted} */
    private void writeDeleted(DataOutput out,
        ReferenceMap referenceMap) throws IOException {
      final List<INode> deleted = getList(ListType.DELETED);
      out.writeInt(deleted.size());
      for (INode node : deleted) {
        FSImageSerialization.saveINode2Image(node, out, true, referenceMap);
      }
    }
    
    /** Serialize to out */
    private void write(DataOutput out, ReferenceMap referenceMap
        ) throws IOException {
      writeCreated(out);
      writeDeleted(out, referenceMap);    
    }

    /** @return The list of INodeDirectory contained in the deleted list */
    private List<INodeDirectory> getDirsInDeleted() {
      List<INodeDirectory> dirList = new ArrayList<INodeDirectory>();
      for (INode node : getList(ListType.DELETED)) {
        if (node.isDirectory()) {
          dirList.add(node.asDirectory());
        }
      }
      return dirList;
    }
    
    /**
     * Interpret the diff and generate a list of {@link DiffReportEntry}.
     * @param parentPath The relative path of the parent.
     * @param parent The directory that the diff belongs to.
     * @param fromEarlier True indicates {@code diff=later-earlier}, 
     *                    False indicates {@code diff=earlier-later}
     * @return A list of {@link DiffReportEntry} as the diff report.
     */
    public List<DiffReportEntry> generateReport(byte[][] parentPath,
        INodeDirectoryWithSnapshot parent, boolean fromEarlier) {
      List<DiffReportEntry> cList = new ArrayList<DiffReportEntry>();
      List<DiffReportEntry> dList = new ArrayList<DiffReportEntry>();
      int c = 0, d = 0;
      List<INode> created = getList(ListType.CREATED);
      List<INode> deleted = getList(ListType.DELETED);
      byte[][] fullPath = new byte[parentPath.length + 1][];
      System.arraycopy(parentPath, 0, fullPath, 0, parentPath.length);
      for (; c < created.size() && d < deleted.size(); ) {
        INode cnode = created.get(c);
        INode dnode = deleted.get(d);
        if (cnode.equals(dnode)) {
          fullPath[fullPath.length - 1] = cnode.getLocalNameBytes();
          if (cnode.isSymlink() && dnode.isSymlink()) {
            dList.add(new DiffReportEntry(DiffType.MODIFY, fullPath));
          } else {
            // must be the case: delete first and then create an inode with the
            // same name
            cList.add(new DiffReportEntry(DiffType.CREATE, fullPath));
            dList.add(new DiffReportEntry(DiffType.DELETE, fullPath));
          }
          c++;
          d++;
        } else if (cnode.compareTo(dnode.getLocalNameBytes()) < 0) {
          fullPath[fullPath.length - 1] = cnode.getLocalNameBytes();
          cList.add(new DiffReportEntry(fromEarlier ? DiffType.CREATE
              : DiffType.DELETE, fullPath));
          c++;
        } else {
          fullPath[fullPath.length - 1] = dnode.getLocalNameBytes();
          dList.add(new DiffReportEntry(fromEarlier ? DiffType.DELETE
              : DiffType.CREATE, fullPath));
          d++;
        }
      }
      for (; d < deleted.size(); d++) {
        fullPath[fullPath.length - 1] = deleted.get(d).getLocalNameBytes();
        dList.add(new DiffReportEntry(fromEarlier ? DiffType.DELETE
            : DiffType.CREATE, fullPath));
      }
      for (; c < created.size(); c++) {
        fullPath[fullPath.length - 1] = created.get(c).getLocalNameBytes();
        cList.add(new DiffReportEntry(fromEarlier ? DiffType.CREATE
            : DiffType.DELETE, fullPath));
      }
      dList.addAll(cList);
      return dList;
    }
  }
  
  /**
   * The difference of an {@link INodeDirectory} between two snapshots.
   */
  static class DirectoryDiff extends
      AbstractINodeDiff<INodeDirectory, DirectoryDiff> {
    /** The size of the children list at snapshot creation time. */
    private final int childrenSize;
    /** The children list diff. */
    private final ChildrenDiff diff;

    private DirectoryDiff(Snapshot snapshot, INodeDirectory dir) {
      super(snapshot, null, null);

      this.childrenSize = dir.getChildrenList(null).size();
      this.diff = new ChildrenDiff();
    }

    /** Constructor used by FSImage loading */
    DirectoryDiff(Snapshot snapshot, INodeDirectory snapshotINode,
        DirectoryDiff posteriorDiff, int childrenSize,
        List<INode> createdList, List<INode> deletedList) {
      super(snapshot, snapshotINode, posteriorDiff);
      this.childrenSize = childrenSize;
      this.diff = new ChildrenDiff(createdList, deletedList);
    }
    
    ChildrenDiff getChildrenDiff() {
      return diff;
    }
    
    /** Is the inode the root of the snapshot? */
    boolean isSnapshotRoot() {
      return snapshotINode == snapshot.getRoot();
    }
    
    @Override
    Quota.Counts combinePosteriorAndCollectBlocks(
        final INodeDirectory currentDir, final DirectoryDiff posterior,
        final BlocksMapUpdateInfo collectedBlocks) {
      final Quota.Counts counts = Quota.Counts.newInstance();
      diff.combinePosterior(posterior.diff, new Diff.Processor<INode>() {
        /** Collect blocks for deleted files. */
        @Override
        public void process(INode inode) {
          if (inode != null) {
            if (INodeReference.tryRemoveReference(inode) <= 0) {
              inode.computeQuotaUsage(counts, false);
              inode.destroyAndCollectBlocks(collectedBlocks);
            } else {
              // if the node is a reference node, we should continue the 
              // snapshot deletion process
              try {
                // use null as prior here because we are handling a reference
                // node stored in the created list of a snapshot diff. This 
                // snapshot diff must be associated with the latest snapshot of
                // the dst tree before the rename operation. In this scenario,
                // the prior snapshot should be the one created in the src tree,
                // and it can be identified by the cleanSubtree since we call
                // recordModification before the rename.
                counts.add(inode.cleanSubtree(posterior.snapshot, null,
                    collectedBlocks));
              } catch (QuotaExceededException e) {
                String error = "should not have QuotaExceededException while deleting snapshot";
                LOG.error(error, e);
              }
            }
          }
        }
      });
      return counts;
    }

    /**
     * @return The children list of a directory in a snapshot.
     *         Since the snapshot is read-only, the logical view of the list is
     *         never changed although the internal data structure may mutate.
     */
    ReadOnlyList<INode> getChildrenList(final INodeDirectory currentDir) {
      return new ReadOnlyList<INode>() {
        private List<INode> children = null;

        private List<INode> initChildren() {
          if (children == null) {
            final ChildrenDiff combined = new ChildrenDiff();
            for(DirectoryDiff d = DirectoryDiff.this; d != null; d = d.getPosterior()) {
              combined.combinePosterior(d.diff, null);
            }
            children = combined.apply2Current(ReadOnlyList.Util.asList(
                currentDir.getChildrenList(null)));
          }
          return children;
        }

        @Override
        public Iterator<INode> iterator() {
          return initChildren().iterator();
        }
    
        @Override
        public boolean isEmpty() {
          return childrenSize == 0;
        }
    
        @Override
        public int size() {
          return childrenSize;
        }
    
        @Override
        public INode get(int i) {
          return initChildren().get(i);
        }
      };
    }

    /** @return the child with the given name. */
    INode getChild(byte[] name, boolean checkPosterior, INodeDirectory currentDir) {
      for(DirectoryDiff d = this; ; d = d.getPosterior()) {
        final Container<INode> returned = d.diff.accessPrevious(name);
        if (returned != null) {
          // the diff is able to determine the inode
          return returned.getElement(); 
        } else if (!checkPosterior) {
          // Since checkPosterior is false, return null, i.e. not found.   
          return null;
        } else if (d.getPosterior() == null) {
          // no more posterior diff, get from current inode.
          return currentDir.getChild(name, null);
        }
      }
    }
    
    @Override
    public String toString() {
      return super.toString() + " childrenSize=" + childrenSize + ", " + diff;
    }
    
    @Override
    void write(DataOutput out, ReferenceMap referenceMap) throws IOException {
      writeSnapshot(out);
      out.writeInt(childrenSize);

      // write snapshotINode
      if (isSnapshotRoot()) {
        out.writeBoolean(true);
      } else {
        out.writeBoolean(false);
        if (snapshotINode != null) {
          out.writeBoolean(true);
          FSImageSerialization.writeINodeDirectory(snapshotINode, out);
        } else {
          out.writeBoolean(false);
        }
      }
      // Write diff. Node need to write poseriorDiff, since diffs is a list.
      diff.write(out, referenceMap);
    }

    @Override
    Quota.Counts destroyDiffAndCollectBlocks(INodeDirectory currentINode,
        BlocksMapUpdateInfo collectedBlocks) {
      // this diff has been deleted
      Quota.Counts counts = Quota.Counts.newInstance();
      List<INodeReference> refNodes = new ArrayList<INodeReference>();
      counts.add(diff.destroyDeletedList(collectedBlocks, refNodes));
      for (INodeReference ref : refNodes) {
        // if the node is a reference node, we should continue the 
        // snapshot deletion process
        try {
          // Use null as prior snapshot. We are handling a reference node stored
          // in the delete list of this snapshot diff. We need to destroy this 
          // snapshot diff because it is the very first one in history.
          // If the ref node is a WithName instance acting as the src node of
          // the rename operation, there will not be any snapshot before the
          // snapshot to be deleted. If the ref node presents the dst node of a 
          // rename operation, we can identify the corresponding prior snapshot 
          // when we come into the subtree of the ref node.
          counts.add(ref.cleanSubtree(this.snapshot, null, collectedBlocks));
        } catch (QuotaExceededException e) {
          String error = 
              "should not have QuotaExceededException while deleting snapshot " 
              + this.snapshot;
          LOG.error(error, e);
        }
      }
      return counts;
    }
  }

  static class DirectoryDiffFactory
      extends AbstractINodeDiff.Factory<INodeDirectory, DirectoryDiff> {
    static final DirectoryDiffFactory INSTANCE = new DirectoryDiffFactory();

    @Override
    DirectoryDiff createDiff(Snapshot snapshot, INodeDirectory currentDir) {
      return new DirectoryDiff(snapshot, currentDir);
    }

    @Override
    INodeDirectory createSnapshotCopy(INodeDirectory currentDir) {
      final INodeDirectory copy = currentDir.isQuotaSet()?
          new INodeDirectoryWithQuota(currentDir, false,
              currentDir.getNsQuota(), currentDir.getDsQuota())
        : new INodeDirectory(currentDir, false);
      copy.clearChildren();
      return copy;
    }
  }

  /** A list of directory diffs. */
  static class DirectoryDiffList
      extends AbstractINodeDiffList<INodeDirectory, DirectoryDiff> {
    DirectoryDiffList() {
      setFactory(DirectoryDiffFactory.INSTANCE);
    }
    
    /** Replace the given child in the created/deleted list, if there is any. */
    private boolean replaceChild(final ListType type, final INode oldChild,
        final INode newChild) {
      final List<DirectoryDiff> diffList = asList();
      for(int i = diffList.size() - 1; i >= 0; i--) {
        final ChildrenDiff diff = diffList.get(i).diff;
        if (diff.replace(type, oldChild, newChild)) {
          return true;
        }
      }
      return false;
    }
    
    /** Remove the given child in the created/deleted list, if there is any. */
    private boolean removeChild(final ListType type, final INode child) {
      final List<DirectoryDiff> diffList = asList();
      for(int i = diffList.size() - 1; i >= 0; i--) {
        final ChildrenDiff diff = diffList.get(i).diff;
        if (diff.removeChild(type, child)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Compute the difference between Snapshots.
   * 
   * @param fromSnapshot Start point of the diff computation. Null indicates
   *          current tree.
   * @param toSnapshot End point of the diff computation. Null indicates current
   *          tree.
   * @param diff Used to capture the changes happening to the children. Note
   *          that the diff still represents (later_snapshot - earlier_snapshot)
   *          although toSnapshot can be before fromSnapshot.
   * @return Whether changes happened between the startSnapshot and endSnaphsot.
   */
  boolean computeDiffBetweenSnapshots(Snapshot fromSnapshot,
      Snapshot toSnapshot, ChildrenDiff diff) {
    Snapshot earlierSnapshot = fromSnapshot;
    Snapshot laterSnapshot = toSnapshot;
    if (Snapshot.ID_COMPARATOR.compare(fromSnapshot, toSnapshot) > 0) {
      earlierSnapshot = toSnapshot;
      laterSnapshot = fromSnapshot;
    }
    
    boolean modified = diffs.changedBetweenSnapshots(earlierSnapshot,
        laterSnapshot);
    if (!modified) {
      return false;
    }
    
    final List<DirectoryDiff> difflist = diffs.asList();
    final int size = difflist.size();
    int earlierDiffIndex = Collections.binarySearch(difflist, earlierSnapshot);
    int laterDiffIndex = laterSnapshot == null ? size : Collections
        .binarySearch(difflist, laterSnapshot);
    earlierDiffIndex = earlierDiffIndex < 0 ? (-earlierDiffIndex - 1)
        : earlierDiffIndex;
    laterDiffIndex = laterDiffIndex < 0 ? (-laterDiffIndex - 1)
        : laterDiffIndex;
    
    boolean dirMetadataChanged = false;
    INodeDirectory dirCopy = null;
    for (int i = earlierDiffIndex; i < laterDiffIndex; i++) {
      DirectoryDiff sdiff = difflist.get(i);
      diff.combinePosterior(sdiff.diff, null);
      if (dirMetadataChanged == false && sdiff.snapshotINode != null) {
        if (dirCopy == null) {
          dirCopy = sdiff.snapshotINode;
        } else {
          if (!dirCopy.metadataEquals(sdiff.snapshotINode)) {
            dirMetadataChanged = true;
          }
        }
      }
    }

    if (!diff.isEmpty() || dirMetadataChanged) {
      return true;
    } else if (dirCopy != null) {
      for (int i = laterDiffIndex; i < size; i++) {
        if (!dirCopy.metadataEquals(difflist.get(i).snapshotINode)) {
          return true;
        }
      }
      return !dirCopy.metadataEquals(this);
    }
    return false;
  }

  /** Diff list sorted by snapshot IDs, i.e. in chronological order. */
  private final DirectoryDiffList diffs;

  public INodeDirectoryWithSnapshot(INodeDirectory that) {
    this(that, true, that instanceof INodeDirectoryWithSnapshot?
        ((INodeDirectoryWithSnapshot)that).getDiffs(): null);
  }

  INodeDirectoryWithSnapshot(INodeDirectory that, boolean adopt,
      DirectoryDiffList diffs) {
    super(that, adopt, that.getNsQuota(), that.getDsQuota());
    this.diffs = diffs != null? diffs: new DirectoryDiffList();
  }

  /** @return the last snapshot. */
  public Snapshot getLastSnapshot() {
    return diffs.getLastSnapshot();
  }

  /** @return the snapshot diff list. */
  DirectoryDiffList getDiffs() {
    return diffs;
  }

  @Override
  public INodeDirectory getSnapshotINode(Snapshot snapshot) {
    return diffs.getSnapshotINode(snapshot, this);
  }

  @Override
  public INodeDirectoryWithSnapshot recordModification(final Snapshot latest)
      throws QuotaExceededException {
    if (isInLatestSnapshot(latest) && !isInSrcSnapshot(latest)) {
      return saveSelf2Snapshot(latest, null);
    }
    return this;
  }

  /** Save the snapshot copy to the latest snapshot. */
  public INodeDirectoryWithSnapshot saveSelf2Snapshot(
      final Snapshot latest, final INodeDirectory snapshotCopy)
          throws QuotaExceededException {
    diffs.saveSelf2Snapshot(latest, this, snapshotCopy);
    return this;
  }

  @Override
  public INode saveChild2Snapshot(final INode child, final Snapshot latest,
      final INode snapshotCopy) throws QuotaExceededException {
    Preconditions.checkArgument(!child.isDirectory(),
        "child is a directory, child=%s", child);
    if (latest == null) {
      return child;
    }

    final DirectoryDiff diff = diffs.checkAndAddLatestSnapshotDiff(latest, this);
    if (diff.getChild(child.getLocalNameBytes(), false, this) != null) {
      // it was already saved in the latest snapshot earlier.  
      return child;
    }

    diff.diff.modify(snapshotCopy, child);
    return child;
  }

  @Override
  public boolean addChild(INode inode, boolean setModTime, Snapshot latest)
      throws QuotaExceededException {
    ChildrenDiff diff = null;
    Integer undoInfo = null;
    if (latest != null) {
      diff = diffs.checkAndAddLatestSnapshotDiff(latest, this).diff;
      undoInfo = diff.create(inode);
    }
    final boolean added = super.addChild(inode, setModTime, null);
    if (!added && undoInfo != null) {
      diff.undoCreate(inode, undoInfo);
    }
    return added; 
  }

  @Override
  public boolean removeChild(INode child, Snapshot latest)
      throws QuotaExceededException {
    ChildrenDiff diff = null;
    UndoInfo<INode> undoInfo = null;
    if (latest != null) {
      diff = diffs.checkAndAddLatestSnapshotDiff(latest, this).diff;
      undoInfo = diff.delete(child);
    }
    final boolean removed = removeChild(child);
    if (undoInfo != null) {
      if (!removed) {
        //remove failed, undo
        diff.undoDelete(child, undoInfo);
      }
    }
    return removed;
  }

  @Override
  public boolean removeChildAndAllSnapshotCopies(INode child) {
    if (!removeChild(child)) {
      return false;
    }

    // remove same child from the created list, if there is any.
    final List<DirectoryDiff> diffList = diffs.asList();
    for(int i = diffList.size() - 1; i >= 0; i--) {
      final ChildrenDiff diff = diffList.get(i).diff;
      if (diff.removeChild(ListType.CREATED, child)) {
        return true;
      }
    }
    return true;
  }
  
  @Override
  public void replaceChild(final INode oldChild, final INode newChild) {
    super.replaceChild(oldChild, newChild);
    diffs.replaceChild(ListType.CREATED, oldChild, newChild);
  }
  
  /**
   * This method is usually called by the undo section of rename.
   * 
   * Before calling this function, in the rename operation, we replace the
   * original src node (of the rename operation) with a reference node (WithName
   * instance) in both the children list and a created list, delete the
   * reference node from the children list, and add it to the corresponding
   * deleted list.
   * 
   * To undo the above operations, we have the following steps in particular:
   * 
   * <pre>
   * 1) remove the WithName node from the deleted list (if it exists) 
   * 2) replace the WithName node in the created list with srcChild 
   * 3) add srcChild back as a child of srcParent. Note that we already add 
   * the node into the created list of a snapshot diff in step 2, we do not need
   * to add srcChild to the created list of the latest snapshot.
   * </pre>
   * 
   * We do not need to update quota usage because the old child is in the 
   * deleted list before. 
   * 
   * @param oldChild
   *          The reference node to be removed/replaced
   * @param newChild
   *          The node to be added back
   * @param latestSnapshot
   *          The latest snapshot. Note this may not be the last snapshot in the
   *          {@link #diffs}, since the src tree of the current rename operation
   *          may be the dst tree of a previous rename.
   * @throws QuotaExceededException should not throw this exception
   */
  public void undoRename4ScrParent(final INodeReference oldChild,
      final INode newChild, Snapshot latestSnapshot)
      throws QuotaExceededException {
    diffs.removeChild(ListType.DELETED, oldChild);
    diffs.replaceChild(ListType.CREATED, oldChild, newChild);
    addChild(newChild, true, null);
  }
  
  /**
   * Undo the rename operation for the dst tree, i.e., if the rename operation
   * (with OVERWRITE option) removes a file/dir from the dst tree, add it back
   * and delete possible record in the deleted list.  
   */
  public void undoRename4DstParent(final INode deletedChild,
      Snapshot latestSnapshot) throws QuotaExceededException {
    boolean removeDeletedChild = diffs.removeChild(ListType.DELETED,
        deletedChild);
    final boolean added = addChild(deletedChild, true, removeDeletedChild ? null
        : latestSnapshot);
    // update quota usage if adding is successfully and the old child has not
    // been stored in deleted list before
    if (added && !removeDeletedChild) {
      final Quota.Counts counts = deletedChild.computeQuotaUsage();
      addSpaceConsumed(counts.get(Quota.NAMESPACE),
          counts.get(Quota.DISKSPACE), false);
    }
  }

  @Override
  public ReadOnlyList<INode> getChildrenList(Snapshot snapshot) {
    final DirectoryDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.getChildrenList(this): super.getChildrenList(null);
  }

  @Override
  public INode getChild(byte[] name, Snapshot snapshot) {
    final DirectoryDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.getChild(name, true, this): super.getChild(name, null);
  }

  @Override
  public String toDetailString() {
    return super.toDetailString() + ", " + diffs;
  }
  
  /**
   * Get all the directories that are stored in some snapshot but not in the
   * current children list. These directories are equivalent to the directories
   * stored in the deletes lists.
   * 
   * @param snapshotDirMap A snapshot-to-directory-list map for returning.
   * @return The number of directories returned.
   */
  public int getSnapshotDirectory(
      Map<Snapshot, List<INodeDirectory>> snapshotDirMap) {
    int dirNum = 0;
    for (DirectoryDiff sdiff : diffs) {
      List<INodeDirectory> list = sdiff.getChildrenDiff().getDirsInDeleted();
      if (list.size() > 0) {
        snapshotDirMap.put(sdiff.snapshot, list);
        dirNum += list.size();
      }
    }
    return dirNum;
  }

  @Override
  public Quota.Counts cleanSubtree(final Snapshot snapshot, Snapshot prior,
      final BlocksMapUpdateInfo collectedBlocks)
      throws QuotaExceededException {
    Quota.Counts counts = Quota.Counts.newInstance();
    if (snapshot == null) { // delete the current directory
      recordModification(prior);
      // delete everything in created list
      DirectoryDiff lastDiff = diffs.getLast();
      if (lastDiff != null) {
        counts.add(lastDiff.diff.destroyCreatedList(this, collectedBlocks));
      }
    } else {
      // update prior
      prior = getDiffs().updatePrior(snapshot, prior);
      counts.add(getDiffs().deleteSnapshotDiff(snapshot, prior, this, 
          collectedBlocks));
      if (prior != null) {
        DirectoryDiff priorDiff = this.getDiffs().getDiff(prior);
        if (priorDiff != null) {
          // For files/directories created between "prior" and "snapshot", 
          // we need to clear snapshot copies for "snapshot". Note that we must
          // use null as prior in the cleanSubtree call. Files/directories that
          // were created before "prior" will be covered by the later 
          // cleanSubtreeRecursively call.
          for (INode cNode : priorDiff.getChildrenDiff().getList(
              ListType.CREATED)) {
            counts.add(cNode.cleanSubtree(snapshot, null, collectedBlocks));
          }
          // When a directory is moved from the deleted list of the posterior
          // diff to the deleted list of this diff, we need to destroy its
          // descendants that were 1) created after taking this diff and 2)
          // deleted after taking posterior diff.

          // For files moved from posterior's deleted list, we also need to
          // delete its snapshot copy associated with the posterior snapshot.
          for (INode dNode : priorDiff.getChildrenDiff().getList(
              ListType.DELETED)) {
            counts.add(cleanDeletedINode(dNode, snapshot, prior,
                collectedBlocks));
          }
        }
      }
    }
    counts.add(cleanSubtreeRecursively(snapshot, prior, collectedBlocks));
    
    if (isQuotaSet()) {
      this.addSpaceConsumed2Cache(-counts.get(Quota.NAMESPACE),
          -counts.get(Quota.DISKSPACE));
    }
    return counts;
  }
  
  /**
   * Clean an inode while we move it from the deleted list of post to the
   * deleted list of prior.
   * @param inode The inode to clean.
   * @param post The post snapshot.
   * @param prior The prior snapshot.
   * @param collectedBlocks Used to collect blocks for later deletion.
   * @return Quota usage update.
   */
  private Quota.Counts cleanDeletedINode(INode inode, Snapshot post,
      Snapshot prior, final BlocksMapUpdateInfo collectedBlocks) {
    Quota.Counts counts = Quota.Counts.newInstance();
    Deque<INode> queue = new ArrayDeque<INode>();
    queue.addLast(inode);
    while (!queue.isEmpty()) {
      INode topNode = queue.pollFirst();
      if (topNode instanceof FileWithSnapshot) {
        FileWithSnapshot fs = (FileWithSnapshot) topNode;
        counts.add(fs.getDiffs().deleteSnapshotDiff(post, prior,
            topNode.asFile(), collectedBlocks));
      } else if (topNode.isDirectory()) {
        INodeDirectory dir = topNode.asDirectory();
        if (dir instanceof INodeDirectoryWithSnapshot) {
          // delete files/dirs created after prior. Note that these
          // files/dirs, along with inode, were deleted right after post.
          INodeDirectoryWithSnapshot sdir = (INodeDirectoryWithSnapshot) dir;
          DirectoryDiff priorDiff = sdir.getDiffs().getDiff(prior);
          if (priorDiff != null) {
            counts.add(priorDiff.diff.destroyCreatedList(sdir,
                collectedBlocks));
          }
        }
        for (INode child : dir.getChildrenList(prior)) {
          queue.addLast(child);
        }
      }
    }
    return counts;
  }

  @Override
  public void destroyAndCollectBlocks(
      final BlocksMapUpdateInfo collectedBlocks) {
    // destroy its diff list
    for (DirectoryDiff diff : diffs) {
      diff.destroyDiffAndCollectBlocks(this, collectedBlocks);
    }
    diffs.clear();
    super.destroyAndCollectBlocks(collectedBlocks);
  }

  @Override
  public Quota.Counts computeQuotaUsage4CurrentDirectory(Quota.Counts counts) {
    super.computeQuotaUsage4CurrentDirectory(counts);

    for(DirectoryDiff d : diffs) {
      for(INode deleted : d.getChildrenDiff().getList(ListType.DELETED)) {
        deleted.computeQuotaUsage(counts, false);
      }
    }
    counts.add(Quota.NAMESPACE, diffs.asList().size());
    return counts;
  }

  @Override
  public Content.CountsMap computeContentSummary(
      final Content.CountsMap countsMap) {
    super.computeContentSummary(countsMap);
    computeContentSummary4Snapshot(countsMap.getCounts(Key.SNAPSHOT));
    return countsMap;
  }

  @Override
  public Content.Counts computeContentSummary(final Content.Counts counts) {
    super.computeContentSummary(counts);
    computeContentSummary4Snapshot(counts);
    return counts;
  }

  private void computeContentSummary4Snapshot(final Content.Counts counts) {
    for(DirectoryDiff d : diffs) {
      for(INode deleted : d.getChildrenDiff().getList(ListType.DELETED)) {
        deleted.computeContentSummary(counts);
      }
    }
    counts.add(Content.DIRECTORY, diffs.asList().size());
  }
}
