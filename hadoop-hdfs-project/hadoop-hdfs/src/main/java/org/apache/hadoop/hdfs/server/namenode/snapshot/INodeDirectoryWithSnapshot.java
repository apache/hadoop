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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * The directory with snapshots. It maintains a list of snapshot diffs for
 * storing snapshot data. When there are modifications to the directory, the old
 * data is stored in the latest snapshot, if there is any.
 */
public class INodeDirectoryWithSnapshot extends INodeDirectoryWithQuota {
  /**
   * The difference between the current state and a previous snapshot
   * of an INodeDirectory.
   * 
   * <pre>
   * Two lists are maintained in the algorithm:
   * - c-list for newly created inodes
   * - d-list for the deleted inodes
   *
   * Denote the state of an inode by the following
   *   (0, 0): neither in c-list nor d-list
   *   (c, 0): in c-list but not in d-list
   *   (0, d): in d-list but not in c-list
   *   (c, d): in both c-list and d-list
   *
   * For each case below, ( , ) at the end shows the result state of the inode.
   *
   * Case 1. Suppose the inode i is NOT in the previous snapshot.        (0, 0)
   *   1.1. create i in current: add it to c-list                        (c, 0)
   *   1.1.1. create i in current and then create: impossible
   *   1.1.2. create i in current and then delete: remove it from c-list (0, 0)
   *   1.1.3. create i in current and then modify: replace it in c-list (c', 0)
   *
   *   1.2. delete i from current: impossible
   *
   *   1.3. modify i in current: impossible
   *
   * Case 2. Suppose the inode i is ALREADY in the previous snapshot.    (0, 0)
   *   2.1. create i in current: impossible
   *
   *   2.2. delete i from current: add it to d-list                      (0, d)
   *   2.2.1. delete i from current and then create: add it to c-list    (c, d)
   *   2.2.2. delete i from current and then delete: impossible
   *   2.2.2. delete i from current and then modify: impossible
   *
   *   2.3. modify i in current: put it in both c-list and d-list        (c, d)
   *   2.3.1. modify i in current and then create: impossible
   *   2.3.2. modify i in current and then delete: remove it from c-list (0, d)
   *   2.3.3. modify i in current and then modify: replace it in c-list (c', d)
   * </pre>
   */
  static class Diff {
    /**
     * Search the inode from the list.
     * @return -1 if the list is null; otherwise, return the insertion point
     *    defined in {@link Collections#binarySearch(List, Object)}.
     *    Note that, when the list is null, -1 is the correct insertion point.
     */
    static int search(final List<INode> inodes, final INode i) {
      return search(inodes, i.getLocalNameBytes());
    }
    private static int search(final List<INode> inodes, final byte[] name) {
      return inodes == null? -1: Collections.binarySearch(inodes, name);
    }

    private static void remove(final List<INode> inodes, final int i,
        final INode expected) {
      final INode removed = inodes.remove(-i - 1);
      Preconditions.checkState(removed == expected,
          "removed != expected=%s, removed=%s.", expected, removed);
    }

    /** c-list: inode(s) created in current. */
    private List<INode> created;
    /** d-list: inode(s) deleted from current. */
    private List<INode> deleted;

    /**
     * Insert the inode to created.
     * @param i the insertion point defined
     *          in {@link Collections#binarySearch(List, Object)}
     */
    private void insertCreated(final INode inode, final int i) {
      if (i >= 0) {
        throw new AssertionError("Inode already exists: inode=" + inode
            + ", created=" + created);
      }
      if (created == null) {
        created = new ArrayList<INode>(DEFAULT_FILES_PER_DIRECTORY);
      }
      created.add(-i - 1, inode);
    }

    /**
     * Insert the inode to deleted.
     * @param i the insertion point defined
     *          in {@link Collections#binarySearch(List, Object)}
     */
    private void insertDeleted(final INode inode, final int i) {
      if (i >= 0) {
        throw new AssertionError("Inode already exists: inode=" + inode
            + ", deleted=" + deleted);
      }
      if (deleted == null) {
        deleted = new ArrayList<INode>(DEFAULT_FILES_PER_DIRECTORY);
      }
      deleted.add(-i - 1, inode);
    }

    /**
     * Create an inode in current state.
     * @return the c-list insertion point for undo.
     */
    int create(final INode inode) {
      final int c = search(created, inode);
      insertCreated(inode, c);
      return c;
    }

    void undoCreate(final INode inode, final int insertionPoint) {
      remove(created, insertionPoint, inode);
    }

    /**
     * Delete an inode from current state.
     * @return a triple for undo.
     */
    Triple<Integer, INode, Integer> delete(final INode inode) {
      final int c = search(created, inode);
      INode previous = null;
      Integer d = null;
      if (c >= 0) {
        // remove a newly created inode
        previous = created.remove(c);
      } else {
        // not in c-list, it must be in previous
        d = search(deleted, inode);
        insertDeleted(inode, d);
      }
      return new Triple<Integer, INode, Integer>(c, previous, d);
    }
    
    void undoDelete(final INode inode,
        final Triple<Integer, INode, Integer> undoInfo) {
      final int c = undoInfo.left;
      if (c >= 0) {
        created.add(c, undoInfo.middle);
      } else {
        remove(deleted, undoInfo.right, inode);
      }
    }

    /**
     * Modify an inode in current state.
     * @return a triple for undo.
     */
    Triple<Integer, INode, Integer> modify(final INode oldinode, final INode newinode) {
      if (!oldinode.equals(newinode)) {
        throw new AssertionError("The names do not match: oldinode="
            + oldinode + ", newinode=" + newinode);
      }
      final int c = search(created, newinode);
      INode previous = null;
      Integer d = null;
      if (c >= 0) {
        // Case 1.1.3 and 2.3.3: inode is already in c-list,
        previous = created.set(c, newinode);
        
        //TODO: fix a bug that previous != oldinode.  Set it to oldinode for now
        previous = oldinode;
      } else {
        d = search(deleted, oldinode);
        if (d < 0) {
          // Case 2.3: neither in c-list nor d-list
          insertCreated(newinode, c);
          insertDeleted(oldinode, d);
        }
      }
      return new Triple<Integer, INode, Integer>(c, previous, d);
    }

    void undoModify(final INode oldinode, final INode newinode,
        final Triple<Integer, INode, Integer> undoInfo) {
      final int c = undoInfo.left;
      if (c >= 0) {
        created.set(c, undoInfo.middle);
      } else {
        final int d = undoInfo.right;
        if (d < 0) {
          remove(created, c, newinode);
          remove(deleted, d, oldinode);
        }
      }
    }

    /**
     * Find an inode in the previous snapshot.
     * @return null if the inode cannot be determined in the previous snapshot
     *         since no change is recorded and it should be determined in the
     *         current snapshot; otherwise, return an array with size one
     *         containing the inode in the previous snapshot. Note that the
     *         inode can possibly be null which means that the inode is not
     *         found in the previous snapshot.
     */
    INode[] accessPrevious(byte[] name) {
      return accessPrevious(name, created, deleted);
    }

    private static INode[] accessPrevious(byte[] name,
        final List<INode> clist, final List<INode> dlist) {
      final int d = search(dlist, name);
      if (d >= 0) {
        // the inode was in previous and was once deleted in current.
        return new INode[]{dlist.get(d)};
      } else {
        final int c = search(clist, name);
        // When c >= 0, the inode in current is a newly created inode.
        return c >= 0? new INode[]{null}: null;
      }
    }

    /**
     * Find an inode in the current snapshot.
     * @return null if the inode cannot be determined in the current snapshot
     *         since no change is recorded and it should be determined in the
     *         previous snapshot; otherwise, return an array with size one
     *         containing the inode in the current snapshot. Note that the
     *         inode can possibly be null which means that the inode is not
     *         found in the current snapshot.
     */
    INode[] accessCurrent(byte[] name) {
      return accessPrevious(name, deleted, created);
    }

    /**
     * Apply this diff to previous snapshot in order to obtain current state.
     * @return the current state of the list.
     */
    List<INode> apply2Previous(final List<INode> previous) {
      return apply2Previous(previous, created, deleted);
    }

    private static List<INode> apply2Previous(final List<INode> previous,
        final List<INode> clist, final List<INode> dlist) {
      final List<INode> current = new ArrayList<INode>(previous);
      if (dlist != null) {
        for(INode d : dlist) {
          current.remove(d);
        }
      }
      if (clist != null) {
        for(INode c : clist) {
          final int i = search(current, c);
          current.add(-i - 1, c);
        }
      }
      return current;
    }

    /**
     * Apply the reverse of this diff to current state in order
     * to obtain the previous snapshot.
     * @return the previous state of the list.
     */
    List<INode> apply2Current(final List<INode> current) {
      return apply2Previous(current, deleted, created);
    }
    
    /**
     * Combine the posterior diff with this diff. This function needs to called
     * before the posterior diff is to be deleted. In general we have:
     * 
     * <pre>
     * 1. For (c, 0) in the posterior diff, check the inode in this diff:
     * 1.1 (c', 0) in this diff: impossible
     * 1.2 (0, d') in this diff: put in created --> (c, d')
     * 1.3 (c', d') in this diff: impossible
     * 1.4 (0, 0) in this diff: put in created --> (c, 0)
     * This is the same logic with {@link #create(INode)}.
     * 
     * 2. For (0, d) in the posterior diff,
     * 2.1 (c', 0) in this diff: remove from old created --> (0, 0)
     * 2.2 (0, d') in this diff: impossible
     * 2.3 (c', d') in this diff: remove from old created --> (0, d')
     * 2.4 (0, 0) in this diff: put in deleted --> (0, d)
     * This is the same logic with {@link #delete(INode)}.
     * 
     * 3. For (c, d) in the posterior diff,
     * 3.1 (c', 0) in this diff: replace old created --> (c, 0)
     * 3.2 (0, d') in this diff: impossible
     * 3.3 (c', d') in this diff: replace old created --> (c, d')
     * 3.4 (0, 0) in this diff: put in created and deleted --> (c, d)
     * This is the same logic with {@link #modify(INode, INode)}.
     * </pre>
     * 
     * Note that after this function the postDiff will be deleted.
     * 
     * @param the posterior diff to combine
     * @param deletedINodeProcesser Used in case 2.1, 2.3, 3.1, and 3.3
     *                              to process the deleted inodes.
     */
    void combinePostDiff(Diff postDiff, Processor deletedINodeProcesser) {
      final List<INode> postCreated = postDiff.created != null?
          postDiff.created: Collections.<INode>emptyList();
      final List<INode> postDeleted = postDiff.deleted != null?
          postDiff.deleted: Collections.<INode>emptyList();
      final Iterator<INode> createdIterator = postCreated.iterator();
      final Iterator<INode> deletedIterator = postDeleted.iterator();

      INode c = createdIterator.hasNext()? createdIterator.next(): null;
      INode d = deletedIterator.hasNext()? deletedIterator.next(): null;

      for(; c != null || d != null; ) {
        final int cmp = c == null? 1
            : d == null? -1
            : c.compareTo(d.getLocalNameBytes());
        if (cmp < 0) {
          // case 1: only in c-list
          create(c);
          c = createdIterator.hasNext()? createdIterator.next(): null;
        } else if (cmp > 0) {
          // case 2: only in d-list
          Triple<Integer, INode, Integer> triple = delete(d);
          if (deletedINodeProcesser != null) {
            deletedINodeProcesser.process(triple.middle);
          }
          d = deletedIterator.hasNext()? deletedIterator.next(): null;
        } else {
          // case 3: in both c-list and d-list 
          final Triple<Integer, INode, Integer> triple = modify(d, c);
          if (deletedINodeProcesser != null) {
            deletedINodeProcesser.process(triple.middle);
          }
          c = createdIterator.hasNext()? createdIterator.next(): null;
          d = deletedIterator.hasNext()? deletedIterator.next(): null;
        }
      }
    }

    /** Convert the inode list to a compact string. */
    static String toString(List<INode> inodes) {
      if (inodes == null || inodes.isEmpty()) {
        return "<empty>";
      }
      final StringBuilder b = new StringBuilder("[")
          .append(inodes.get(0));
      for(int i = 1; i < inodes.size(); i++) {
        b.append(", ").append(inodes.get(i));
      }
      return b.append("]").toString();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName()
          + "{created=" + toString(created)
          + ", deleted=" + toString(deleted) + "}";
    }
  }
  
  /**
   * The difference between two snapshots. {@link INodeDirectoryWithSnapshot}
   * maintains a list of snapshot diffs,
   * <pre>
   *   d_1 -> d_2 -> ... -> d_n -> null,
   * </pre>
   * where -> denotes the {@link SnapshotDiff#posteriorDiff} reference. The
   * current directory state is stored in the field of {@link INodeDirectory}.
   * The snapshot state can be obtained by applying the diffs one-by-one in
   * reversed chronological order.  Let s_1, s_2, ..., s_n be the corresponding
   * snapshots.  Then,
   * <pre>
   *   s_n                     = (current state) - d_n;
   *   s_{n-1} = s_n - d_{n-1} = (current state) - d_n - d_{n-1};
   *   ...
   *   s_k     = s_{k+1} - d_k = (current state) - d_n - d_{n-1} - ... - d_k.
   * </pre>
   */
  class SnapshotDiff implements Comparable<Snapshot> {
    /** The snapshot will be obtained after this diff is applied. */
    final Snapshot snapshot;
    /** The size of the children list at snapshot creation time. */
    final int childrenSize;
    /**
     * Posterior diff is the diff happened after this diff.
     * The posterior diff should be first applied to obtain the posterior
     * snapshot and then apply this diff in order to obtain this snapshot.
     * If the posterior diff is null, the posterior state is the current state. 
     */
    private SnapshotDiff posteriorDiff;
    /** The children list diff. */
    private final Diff diff = new Diff();
    /** The snapshot inode data.  It is null when there is no change. */
    private INodeDirectory snapshotINode = null;

    private SnapshotDiff(Snapshot snapshot, INodeDirectory dir) {
      Preconditions.checkNotNull(snapshot, "snapshot is null");

      this.snapshot = snapshot;
      this.childrenSize = dir.getChildrenList(null).size();
    }

    /** Compare diffs with snapshot ID. */
    @Override
    public int compareTo(final Snapshot that) {
      return Snapshot.ID_COMPARATOR.compare(this.snapshot, that);
    }
    
    /** Is the inode the root of the snapshot? */
    boolean isSnapshotRoot() {
      return snapshotINode == snapshot.getRoot();
    }

    /** Copy the INode state to the snapshot if it is not done already. */
    private Pair<INodeDirectory, INodeDirectory> checkAndInitINode(
        INodeDirectory snapshotCopy) {
      if (snapshotINode != null) {
        // already initialized.
        return null;
      }
      final INodeDirectoryWithSnapshot dir = INodeDirectoryWithSnapshot.this;
      if (snapshotCopy == null) {
        snapshotCopy = new INodeDirectory(dir, false);
      }
      snapshotINode = snapshotCopy;
      return new Pair<INodeDirectory, INodeDirectory>(dir, snapshotCopy);
    }

    /** @return the snapshot object of this diff. */
    Snapshot getSnapshot() {
      return snapshot;
    }

    private INodeDirectory getSnapshotINode() {
      // get from this diff, then the posterior diff and then the current inode
      for(SnapshotDiff d = this; ; d = d.posteriorDiff) {
        if (d.snapshotINode != null) {
          return d.snapshotINode;
        } else if (d.posteriorDiff == null) {
          return INodeDirectoryWithSnapshot.this;
        }
      }
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
            final Diff combined = new Diff();
            for(SnapshotDiff d = SnapshotDiff.this; d != null; d = d.posteriorDiff) {
              combined.combinePostDiff(d.diff, null);
            }
            children = combined.apply2Current(ReadOnlyList.Util.asList(
                INodeDirectoryWithSnapshot.this.getChildrenList(null)));
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
      for(SnapshotDiff d = this; ; d = d.posteriorDiff) {
        final INode[] array = d.diff.accessPrevious(name);
        if (array != null) {
          // the diff is able to find it
          return array[0]; 
        } else if (!checkPosterior) {
          // Since checkPosterior is false, return null, i.e. not found.   
          return null;
        } else if (d.posteriorDiff == null) {
          // no more posterior diff, get from current inode.
          return INodeDirectoryWithSnapshot.this.getChild(name, null);
        }
      }
    }
    
    @Override
    public String toString() {
      return "\n  " + snapshot + " (-> "
          + (posteriorDiff == null? null: posteriorDiff.snapshot)
          + ") childrenSize=" + childrenSize + ", " + diff;
    }
  }
  
  /** An interface for passing a method to process inodes. */
  static interface Processor {
    /** Process the given inode. */
    void process(INode inode);
  }

  /** Create an {@link INodeDirectoryWithSnapshot} with the given snapshot.*/
  public static INodeDirectoryWithSnapshot newInstance(INodeDirectory dir,
      Snapshot latest) {
    final INodeDirectoryWithSnapshot withSnapshot
        = new INodeDirectoryWithSnapshot(dir, true, null);
    if (latest != null) {
      // add a diff for the latest snapshot
      withSnapshot.addSnapshotDiff(latest, dir, false);
    }
    return withSnapshot;
  }

  /** Diff list sorted by snapshot IDs, i.e. in chronological order. */
  private final List<SnapshotDiff> diffs;

  INodeDirectoryWithSnapshot(INodeDirectory that, boolean adopt,
      List<SnapshotDiff> diffs) {
    super(that, adopt, that.getNsQuota(), that.getDsQuota());
    this.diffs = diffs != null? diffs: new ArrayList<SnapshotDiff>();
  }
  
  /**
   * Delete the snapshot with the given name. The synchronization of the diff
   * list will be done outside.
   * 
   * If the diff to remove is not the first one in the diff list, we need to 
   * combine the diff with its previous one:
   * 
   * @param snapshot The snapshot to be deleted
   * @param collectedBlocks Used to collect information for blocksMap update
   * @return The SnapshotDiff containing the deleted snapshot. 
   *         Null if the snapshot with the given name does not exist. 
   */
  SnapshotDiff deleteSnapshotDiff(Snapshot snapshot,
      final BlocksMapUpdateInfo collectedBlocks) {
    int snapshotIndex = Collections.binarySearch(diffs, snapshot);
    if (snapshotIndex == -1) {
      return null;
    } else {
      SnapshotDiff diffToRemove = null;
      diffToRemove = diffs.remove(snapshotIndex);
      if (snapshotIndex > 0) {
        // combine the to-be-removed diff with its previous diff
        SnapshotDiff previousDiff = diffs.get(snapshotIndex - 1);
        previousDiff.diff.combinePostDiff(diffToRemove.diff, new Processor() {
          /** Collect blocks for deleted files. */
          @Override
          public void process(INode inode) {
            if (inode != null && inode instanceof INodeFile) {
              ((INodeFile)inode).collectSubtreeBlocksAndClear(collectedBlocks);
            }
          }
        });

        previousDiff.posteriorDiff = diffToRemove.posteriorDiff;
        diffToRemove.posteriorDiff = null;
      }
      return diffToRemove;
    }
  }

  /** Add a {@link SnapshotDiff} for the given snapshot and directory. */
  SnapshotDiff addSnapshotDiff(Snapshot snapshot, INodeDirectory dir,
      boolean isSnapshotCreation) {
    final SnapshotDiff last = getLastSnapshotDiff();
    final SnapshotDiff d = new SnapshotDiff(snapshot, dir); 

    if (isSnapshotCreation) {
      //for snapshot creation, snapshotINode is the same as the snapshot root
      d.snapshotINode = snapshot.getRoot();
    }
    diffs.add(d);
    if (last != null) {
      last.posteriorDiff = d;
    }
    return d;
  }

  SnapshotDiff getLastSnapshotDiff() {
    final int n = diffs.size();
    return n == 0? null: diffs.get(n - 1);
  }

  /** @return the last snapshot. */
  public Snapshot getLastSnapshot() {
    final SnapshotDiff last = getLastSnapshotDiff();
    return last == null? null: last.getSnapshot();
  }

  /**
   * Check if the latest snapshot diff exists.  If not, add it.
   * @return the latest snapshot diff, which is never null.
   */
  private SnapshotDiff checkAndAddLatestSnapshotDiff(Snapshot latest) {
    final SnapshotDiff last = getLastSnapshotDiff();
    return last != null && last.snapshot.equals(latest)? last
        : addSnapshotDiff(latest, this, false);
  }
  
  /**
   * Check if the latest {@link Diff} exists.  If not, add it.
   * @return the latest {@link Diff}, which is never null.
   */
  Diff checkAndAddLatestDiff(Snapshot latest) {
    return checkAndAddLatestSnapshotDiff(latest).diff;
  }

  /**
   * @return {@link #snapshots}
   */
  @VisibleForTesting
  List<SnapshotDiff> getSnapshotDiffs() {
    return diffs;
  }

  /**
   * @return the diff corresponding to the given snapshot.
   *         When the diff is null, it means that the current state and
   *         the corresponding snapshot state are the same. 
   */
  SnapshotDiff getSnapshotDiff(Snapshot snapshot) {
    if (snapshot == null) {
      // snapshot == null means the current state, therefore, return null.
      return null;
    }
    final int i = Collections.binarySearch(diffs, snapshot);
    if (i >= 0) {
      // exact match
      return diffs.get(i);
    } else {
      // Exact match not found means that there were no changes between
      // given snapshot and the next state so that the diff for the given
      // snapshot was not recorded.  Thus, return the next state.
      final int j = -i - 1;
      return j < diffs.size()? diffs.get(j): null;
    }
  }

  @Override
  public Pair<INodeDirectory, INodeDirectory> recordModification(Snapshot latest) {
    return save2Snapshot(latest, null);
  }

  /** Save the snapshot copy to the latest snapshot. */
  public Pair<INodeDirectory, INodeDirectory> save2Snapshot(Snapshot latest,
      INodeDirectory snapshotCopy) {
    return latest == null? null
        : checkAndAddLatestSnapshotDiff(latest).checkAndInitINode(snapshotCopy);
  }

  @Override
  public Pair<? extends INode, ? extends INode> saveChild2Snapshot(
      INode child, Snapshot latest) {
    Preconditions.checkArgument(!child.isDirectory(),
        "child is a directory, child=%s", child);

    final SnapshotDiff diff = checkAndAddLatestSnapshotDiff(latest);
    if (diff.getChild(child.getLocalNameBytes(), false) != null) {
      // it was already saved in the latest snapshot earlier.  
      return null;
    }

    final Pair<? extends INode, ? extends INode> p = child.createSnapshotCopy();
    diff.diff.modify(p.right, p.left);
    return p;
  }

  @Override
  public boolean addChild(INode inode, boolean setModTime, Snapshot latest) {
    Diff diff = null;
    Integer undoInfo = null;
    if (latest != null) {
      diff = checkAndAddLatestDiff(latest);
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
    Diff diff = null;
    Triple<Integer, INode, Integer> undoInfo = null;
    if (latest != null) {
      diff = checkAndAddLatestDiff(latest);
      undoInfo = diff.delete(child);
    }
    final INode removed = super.removeChild(child, null);
    if (removed == null && undoInfo != null) {
      diff.undoDelete(child, undoInfo);
    }
    return removed;
  }

  @Override
  public ReadOnlyList<INode> getChildrenList(Snapshot snapshot) {
    final SnapshotDiff diff = getSnapshotDiff(snapshot);
    return diff != null? diff.getChildrenList(): super.getChildrenList(null);
  }

  @Override
  public INode getChild(byte[] name, Snapshot snapshot) {
    final SnapshotDiff diff = getSnapshotDiff(snapshot);
    return diff != null? diff.getChild(name, true): super.getChild(name, null);
  }

  @Override
  public String getUserName(Snapshot snapshot) {
    final SnapshotDiff diff = getSnapshotDiff(snapshot);
    return diff != null? diff.getSnapshotINode().getUserName()
        : super.getUserName(null);
  }

  @Override
  public String getGroupName(Snapshot snapshot) {
    final SnapshotDiff diff = getSnapshotDiff(snapshot);
    return diff != null? diff.getSnapshotINode().getGroupName()
        : super.getGroupName(null);
  }

  @Override
  public FsPermission getFsPermission(Snapshot snapshot) {
    final SnapshotDiff diff = getSnapshotDiff(snapshot);
    return diff != null? diff.getSnapshotINode().getFsPermission()
        : super.getFsPermission(null);
  }

  @Override
  public long getAccessTime(Snapshot snapshot) {
    final SnapshotDiff diff = getSnapshotDiff(snapshot);
    return diff != null? diff.getSnapshotINode().getAccessTime()
        : super.getAccessTime(null);
  }

  @Override
  public long getModificationTime(Snapshot snapshot) {
    final SnapshotDiff diff = getSnapshotDiff(snapshot);
    return diff != null? diff.getSnapshotINode().getModificationTime()
        : super.getModificationTime(null);
  }
  
  @Override
  public String toString() {
    return super.toString() + ", diffs=" + getSnapshotDiffs();
  }
}
