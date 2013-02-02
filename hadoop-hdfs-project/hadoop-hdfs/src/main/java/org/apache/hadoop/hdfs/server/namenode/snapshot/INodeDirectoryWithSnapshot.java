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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.diff.Diff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.diff.Diff.Container;
import org.apache.hadoop.hdfs.server.namenode.snapshot.diff.Diff.UndoInfo;
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

    /** Serialize {@link #created} */
    private void writeCreated(DataOutputStream out) throws IOException {
        final List<INode> created = getCreatedList();
        out.writeInt(created.size());
        for (INode node : created) {
          // For INode in created list, we only need to record its local name 
          byte[] name = node.getLocalNameBytes();
          out.writeShort(name.length);
          out.write(name);
        }
    }
    
    /** Serialize {@link #deleted} */
    private void writeDeleted(DataOutputStream out) throws IOException {
        final List<INode> deleted = getDeletedList();
        out.writeInt(deleted.size());
        for (INode node : deleted) {
          if (node.isDirectory()) {
            FSImageSerialization.writeINodeDirectory((INodeDirectory) node, out);
          } else { // INodeFile
            final List<INode> created = getCreatedList();
            // we write the block information only for INodeFile node when the
            // node is only stored in the deleted list or the node is not a
            // snapshot copy
            int createdIndex = search(created, node.getKey());
            if (createdIndex < 0) {
              FSImageSerialization.writeINodeFile((INodeFile) node, out, true);
            } else {
              INodeFile cNode = (INodeFile) created.get(createdIndex);
              INodeFile dNode = (INodeFile) node;
              // A corner case here: after deleting a Snapshot, when combining
              // SnapshotDiff, we may put two inodes sharing the same name but
              // with totally different blocks in the created and deleted list of
              // the same SnapshotDiff.
              if (INodeFile.isOfSameFile(cNode, dNode)) {
                FSImageSerialization.writeINodeFile(dNode, out, false);
              } else {
                FSImageSerialization.writeINodeFile(dNode, out, true);
              }
            }
          }
        }
    }
    
    /** Serialize to out */
    private void write(DataOutputStream out) throws IOException {
      writeCreated(out);
      writeDeleted(out);    
    }
    
    /** @return The list of INodeDirectory contained in the deleted list */
    private List<INodeDirectory> getDirsInDeleted() {
      List<INodeDirectory> dirList = new ArrayList<INodeDirectory>();
      for (INode node : getDeletedList()) {
        if (node.isDirectory()) {
          dirList.add((INodeDirectory) node);
        }
      }
      return dirList;
    }
    
    /**
     * Interpret the diff and generate a list of {@link DiffReportEntry}.
     * @param parent The directory that the diff belongs to.
     * @param fromEarlier True indicates {@code diff=later-earlier}, 
     *                            False indicates {@code diff=earlier-later}
     * @return A list of {@link DiffReportEntry} as the diff report.
     */
    public List<DiffReportEntry> generateReport(
        INodeDirectoryWithSnapshot parent, boolean fromEarlier) {
      List<DiffReportEntry> mList = new ArrayList<DiffReportEntry>();
      List<DiffReportEntry> cList = new ArrayList<DiffReportEntry>();
      List<DiffReportEntry> dList = new ArrayList<DiffReportEntry>();
      int c = 0, d = 0;
      List<INode> created = getCreatedList();
      List<INode> deleted = getDeletedList();
      for (; c < created.size() && d < deleted.size(); ) {
        INode cnode = created.get(c);
        INode dnode = deleted.get(d);
        if (cnode.equals(dnode)) {
          mList.add(new DiffReportEntry(DiffType.MODIFY, parent
              .getFullPathName() + Path.SEPARATOR + cnode.getLocalName()));
          c++;
          d++;
        } else if (cnode.compareTo(dnode.getLocalNameBytes()) < 0) {
          cList.add(new DiffReportEntry(fromEarlier ? DiffType.CREATE
              : DiffType.DELETE, parent.getFullPathName() + Path.SEPARATOR
              + cnode.getLocalName()));
          c++;
        } else {
          dList.add(new DiffReportEntry(fromEarlier ? DiffType.DELETE
              : DiffType.CREATE, parent.getFullPathName() + Path.SEPARATOR
              + dnode.getLocalName()));
          d++;
        }
      }
      for (; d < deleted.size(); d++) {
        dList.add(new DiffReportEntry(fromEarlier ? DiffType.DELETE
            : DiffType.CREATE, parent.getFullPathName() + Path.SEPARATOR
            + deleted.get(d).getLocalName()));
      }
      for (; c < created.size(); c++) {
        cList.add(new DiffReportEntry(fromEarlier ? DiffType.CREATE
            : DiffType.DELETE, parent.getFullPathName() + Path.SEPARATOR
            + created.get(c).getLocalName()));
      }
      cList.addAll(dList);
      cList.addAll(mList);
      return cList;
    }
  }
  
  /**
   * The difference of an {@link INodeDirectory} between two snapshots.
   */
  class DirectoryDiff extends AbstractINodeDiff<INodeDirectory, DirectoryDiff> {
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
    INodeDirectory getCurrentINode() {
      return INodeDirectoryWithSnapshot.this;
    }

    @Override
    void combinePosteriorAndCollectBlocks(final DirectoryDiff posterior,
        final BlocksMapUpdateInfo collectedBlocks) {
      diff.combinePosterior(posterior.diff, new Diff.Processor<INode>() {
        /** Collect blocks for deleted files. */
        @Override
        public void process(INode inode) {
          if (inode != null && inode instanceof INodeFile) {
            ((INodeFile)inode).destroySubtreeAndCollectBlocks(null,
                collectedBlocks);
          }
        }
      });
    }

    /**
     * @return The children list of a directory in a snapshot.
     *         Since the snapshot is read-only, the logical view of the list is
     *         never changed although the internal data structure may mutate.
     */
    ReadOnlyList<INode> getChildrenList() {
      return new ReadOnlyList<INode>() {
        private List<INode> children = null;

        private List<INode> initChildren() {
          if (children == null) {
            final ChildrenDiff combined = new ChildrenDiff();
            for(DirectoryDiff d = DirectoryDiff.this; d != null; d = d.getPosterior()) {
              combined.combinePosterior(d.diff, null);
            }
            children = combined.apply2Current(ReadOnlyList.Util.asList(
                getCurrentINode().getChildrenList(null)));
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
    INode getChild(byte[] name, boolean checkPosterior) {
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
          return getCurrentINode().getChild(name, null);
        }
      }
    }
    
    @Override
    public String toString() {
      final DirectoryDiff posterior = getPosterior();
      return "\n  " + snapshot + " (-> "
          + (posterior == null? null: posterior.snapshot)
          + ") childrenSize=" + childrenSize + ", " + diff;
    }
    
    /** Serialize fields to out */
    void write(DataOutputStream out) throws IOException {
      out.writeInt(childrenSize);
      // No need to write all fields of Snapshot here, since the snapshot must
      // have been recorded before when writing the FSImage. We only need to
      // record the full path of its root.
      byte[] fullPath = DFSUtil.string2Bytes(snapshot.getRoot()
          .getFullPathName());
      out.writeShort(fullPath.length);
      out.write(fullPath);
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
      diff.write(out);
    }
  }

  /** A list of directory diffs. */
  class DirectoryDiffList extends
      AbstractINodeDiffList<INodeDirectory, DirectoryDiff> {
    @Override
    INodeDirectoryWithSnapshot getCurrentINode() {
      return INodeDirectoryWithSnapshot.this;
    }

    @Override
    DirectoryDiff addSnapshotDiff(Snapshot snapshot, INodeDirectory dir,
        boolean isSnapshotCreation) {
      final DirectoryDiff d = new DirectoryDiff(snapshot, dir); 
      if (isSnapshotCreation) {
        //for snapshot creation, snapshotINode is the same as the snapshot root
        d.snapshotINode = snapshot.getRoot();
      }
      return append(d);
    }
  }

  /** Create an {@link INodeDirectoryWithSnapshot} with the given snapshot.*/
  public static INodeDirectoryWithSnapshot newInstance(INodeDirectory dir,
      Snapshot latest) {
    final INodeDirectoryWithSnapshot withSnapshot
        = new INodeDirectoryWithSnapshot(dir, true, null);
    if (latest != null) {
      // add a diff for the latest snapshot
      withSnapshot.diffs.addSnapshotDiff(latest, dir, false);
    }
    return withSnapshot;
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
    if (fromSnapshot == null
        || (toSnapshot != null && Snapshot.ID_COMPARATOR.compare(fromSnapshot,
            toSnapshot) > 0)) {
      earlierSnapshot = toSnapshot;
      laterSnapshot = fromSnapshot;
    }
    
    final List<DirectoryDiff> difflist = diffs.asList();
    final int size = difflist.size();
    int earlierDiffIndex = Collections.binarySearch(difflist, earlierSnapshot);
    if (earlierDiffIndex < 0 && (-earlierDiffIndex - 1) == size) {
      // if the earlierSnapshot is after the latest SnapshotDiff stored in diffs,
      // no modification happened after the earlierSnapshot
      return false;
    }
    int laterDiffIndex = size;
    if (laterSnapshot != null) {
      laterDiffIndex = Collections.binarySearch(difflist, laterSnapshot);
      if (laterDiffIndex == -1 || laterDiffIndex == 0) {
        // if the endSnapshot is the earliest SnapshotDiff stored in
        // diffs, or before it, no modification happened before the endSnapshot
        return false;
      }
    }
    
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
  public Pair<INodeDirectoryWithSnapshot, INodeDirectory> createSnapshotCopy() {
    return new Pair<INodeDirectoryWithSnapshot, INodeDirectory>(this,
        new INodeDirectory(this, false));
  }

  @Override
  public INodeDirectoryWithSnapshot recordModification(Snapshot latest) {
    saveSelf2Snapshot(latest, null);
    return this;
  }

  /** Save the snapshot copy to the latest snapshot. */
  public void saveSelf2Snapshot(Snapshot latest, INodeDirectory snapshotCopy) {
    if (latest != null) {
      diffs.checkAndAddLatestSnapshotDiff(latest).checkAndInitINode(snapshotCopy);
    }
  }

  @Override
  public INode saveChild2Snapshot(INode child, Snapshot latest) {
    Preconditions.checkArgument(!child.isDirectory(),
        "child is a directory, child=%s", child);
    if (latest == null) {
      return child;
    }

    final DirectoryDiff diff = diffs.checkAndAddLatestSnapshotDiff(latest);
    if (diff.getChild(child.getLocalNameBytes(), false) != null) {
      // it was already saved in the latest snapshot earlier.  
      return child;
    }

    final Pair<? extends INode, ? extends INode> p = child.createSnapshotCopy();
    if (p.left != p.right) {
      final UndoInfo<INode> undoIndo = diff.diff.modify(p.right, p.left);
      if (undoIndo.getTrashedElement() != null && p.left instanceof FileWithSnapshot) {
        // also should remove oldinode from the circular list
        FileWithSnapshot newNodeWithLink = (FileWithSnapshot) p.left;
        FileWithSnapshot oldNodeWithLink = (FileWithSnapshot) p.right;
        newNodeWithLink.setNext(oldNodeWithLink.getNext());
        oldNodeWithLink.setNext(null);
      }
    }
    return p.left;
  }

  @Override
  public boolean addChild(INode inode, boolean setModTime, Snapshot latest) {
    ChildrenDiff diff = null;
    Integer undoInfo = null;
    if (latest != null) {
      diff = diffs.checkAndAddLatestSnapshotDiff(latest).diff;
      undoInfo = diff.create(inode);
    }
    final boolean added = super.addChild(inode, setModTime, null);
    if (!added && undoInfo != null) {
      diff.undoCreate(inode, undoInfo);
    }
    return added; 
  }

  @Override
  public INode removeChild(INode child, Snapshot latest) {
    ChildrenDiff diff = null;
    UndoInfo<INode> undoInfo = null;
    if (latest != null) {
      diff = diffs.checkAndAddLatestSnapshotDiff(latest).diff;
      undoInfo = diff.delete(child);
    }
    final INode removed = super.removeChild(child, null);
    if (undoInfo != null) {
      if (removed == null) {
        //remove failed, undo
        diff.undoDelete(child, undoInfo);
      } else {
        //clean up the previously created file, if there is any.
        final INode trashed = undoInfo.getTrashedElement();
        if (trashed != null && trashed instanceof FileWithSnapshot) {
          ((FileWithSnapshot)trashed).removeSelf();
        }
      }
    }
    return removed;
  }
  
  @Override
  public ReadOnlyList<INode> getChildrenList(Snapshot snapshot) {
    final DirectoryDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.getChildrenList(): super.getChildrenList(null);
  }

  @Override
  public INode getChild(byte[] name, Snapshot snapshot) {
    final DirectoryDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.getChild(name, true): super.getChild(name, null);
  }

  @Override
  public String getUserName(Snapshot snapshot) {
    final DirectoryDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.getSnapshotINode().getUserName()
        : super.getUserName(null);
  }

  @Override
  public String getGroupName(Snapshot snapshot) {
    final DirectoryDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.getSnapshotINode().getGroupName()
        : super.getGroupName(null);
  }

  @Override
  public FsPermission getFsPermission(Snapshot snapshot) {
    final DirectoryDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.getSnapshotINode().getFsPermission()
        : super.getFsPermission(null);
  }

  @Override
  public long getAccessTime(Snapshot snapshot) {
    final DirectoryDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.getSnapshotINode().getAccessTime()
        : super.getAccessTime(null);
  }

  @Override
  public long getModificationTime(Snapshot snapshot) {
    final DirectoryDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.getSnapshotINode().getModificationTime()
        : super.getModificationTime(null);
  }
  
  @Override
  public String toString() {
    return super.toString() + ", " + diffs;
  }
  
  /**
   * Get all the INodeDirectory stored in the deletes lists.
   * 
   * @param snapshotDirMap
   *          A HashMap storing all the INodeDirectory stored in the deleted
   *          lists, with their associated full Snapshot.
   * @return The number of INodeDirectory returned.
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
  public int destroySubtreeAndCollectBlocks(final Snapshot snapshot,
      final BlocksMapUpdateInfo collectedBlocks) {
    final int n = super.destroySubtreeAndCollectBlocks(snapshot, collectedBlocks);
    if (snapshot != null) {
      getDiffs().deleteSnapshotDiff(snapshot, collectedBlocks);
    }
    return n;
  }
}
