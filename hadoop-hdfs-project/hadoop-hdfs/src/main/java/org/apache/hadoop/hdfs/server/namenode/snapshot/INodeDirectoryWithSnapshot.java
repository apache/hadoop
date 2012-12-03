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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import com.google.common.base.Preconditions;

/** The directory with snapshots. */
public class INodeDirectoryWithSnapshot extends INodeDirectory {
  /**
   * The difference between the current state and a previous snapshot
   * of an INodeDirectory.
   *
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
   *   1.1.3. create i in current and then modify: replace it in c-list  (c, 0)
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
   *   2.3.3. modify i in current and then modify: replace it in c-list  (c, d)
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

    /** Create an inode in current state. */
    void create(final INode inode) {
      final int c = search(created, inode);
      insertCreated(inode, c);
    }

    /** Delete an inode from current state. */
    void delete(final INode inode) {
      final int c = search(created, inode);
      if (c >= 0) {
        // remove a newly created inode
        created.remove(c);
      } else {
        // not in c-list, it must be in previous
        final int d = search(deleted, inode);
        insertDeleted(inode, d);
      }
    }

    /** Modify an inode in current state. */
    void modify(final INode oldinode, final INode newinode) {
      if (!oldinode.equals(newinode)) {
        throw new AssertionError("The names do not match: oldinode="
            + oldinode + ", newinode=" + newinode);
      }
      final int c = search(created, newinode);
      if (c >= 0) {
        // inode is already in c-list,
        created.set(c, newinode);
      } else {
        final int d = search(deleted, oldinode);
        if (d < 0) {
          // neither in c-list nor d-list
          insertCreated(newinode, c);
          insertDeleted(oldinode, d);
        }
      }
    }

    /**
     * Given an inode in current state, find the corresponding inode in previous
     * snapshot. The inodes in current state and previous snapshot can possibly
     * be the same.
     *
     * @param inodeInCurrent The inode, possibly null, in current state.
     * @return null if the inode is not found in previous snapshot;
     *         otherwise, return the corresponding inode in previous snapshot.
     */
    INode accessPrevious(byte[] name, INode inodeInCurrent) {
      return accessPrevious(name, inodeInCurrent, created, deleted);
    }

    private static INode accessPrevious(byte[] name, INode inodeInCurrent,
        final List<INode> clist, final List<INode> dlist) {
      final int d = search(dlist, name);
      if (d >= 0) {
        // the inode was in previous and was once deleted in current.
        return dlist.get(d);
      } else {
        final int c = search(clist, name);
        // When c >= 0, the inode in current is a newly created inode.
        return c >= 0? null: inodeInCurrent;
      }
    }

    /**
     * Given an inode in previous snapshot, find the corresponding inode in
     * current state. The inodes in current state and previous snapshot can
     * possibly be the same.
     *
     * @param inodeInPrevious The inode, possibly null, in previous snapshot.
     * @return null if the inode is not found in current state;
     *         otherwise, return the corresponding inode in current state.
     */
    INode accessCurrent(byte[] name, INode inodeInPrevious) {
      return accessPrevious(name, inodeInPrevious, deleted, created);
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

    /** Convert the inode list to a compact string. */
    static String toString(List<INode> inodes) {
      if (inodes == null) {
        return null;
      } else if (inodes.isEmpty()) {
        return "[]";
      }
      final StringBuilder b = new StringBuilder("[")
          .append(inodes.get(0).getLocalName());
      for(int i = 1; i < inodes.size(); i++) {
        b.append(", ").append(inodes.get(i).getLocalName());
      }
      return b.append("]").toString();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName()
          + ":\n  created=" + toString(created)
          + "\n  deleted=" + toString(deleted);
    }
  }
  
  private class SnapshotDiff implements Comparable<Snapshot> {
    /** The snapshot will obtain after applied this diff. */
    final Snapshot snapshot;
    /** The size of the children list which is never changed. */
    final int size;
    /**
     * Posterior diff is the diff happened after this diff.
     * The posterior diff should be first applied to obtain the posterior
     * snapshot and then apply this diff in order to obtain this snapshot.
     * If the posterior diff is null, the posterior state is the current state. 
     */
    private SnapshotDiff posteriorDiff;
    /** The data of this diff. */
    private final Diff diff = new Diff();
    /** The snapshot version of the inode. */
    private INode snapshotINode;

    SnapshotDiff(Snapshot snapshot, int size) {
      if (size < 0) {
        throw new HadoopIllegalArgumentException("size = " + size + " < 0");
      }
      this.snapshot = snapshot;
      this.size = size;
    }

    @Override
    public int compareTo(final Snapshot that_snapshot) {
      return Snapshot.ID_COMPARATOR.compare(this.snapshot, that_snapshot);
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
            final ReadOnlyList<INode> posterior = posteriorDiff != null?
                posteriorDiff.getChildrenList()
                : INodeDirectoryWithSnapshot.this.getChildrenList(null);
            children = diff.apply2Current(ReadOnlyList.Util.asList(posterior));
          }
          return children;
        }

        @Override
        public Iterator<INode> iterator() {
          return initChildren().iterator();
        }
    
        @Override
        public boolean isEmpty() {
          return size == 0;
        }
    
        @Override
        public int size() {
          return size;
        }
    
        @Override
        public INode get(int i) {
          return initChildren().get(i);
        }
      };
    }
    
    INode getChild(byte[] name) {
      final INode i = diff.accessPrevious(name, INode.DUMMY);
      if (i != INode.DUMMY) {
        // this diff is able to find it
        return i; 
      } else {
        // should return the posterior INode.
        return posteriorDiff != null? posteriorDiff.getChild(name)
            : INodeDirectoryWithSnapshot.this.getChild(name, null);
      }
    }
  }
  
  /** Replace the given directory to an {@link INodeDirectoryWithSnapshot}. */
  public static INodeDirectoryWithSnapshot replaceDir(INodeDirectory oldDir,
      Snapshot latestSnapshot) {
    Preconditions.checkArgument(!(oldDir instanceof INodeDirectoryWithSnapshot),
        "oldDir is already an INodeDirectoryWithSnapshot, oldDir=%s", oldDir);

    final INodeDirectory parent = oldDir.getParent();
    Preconditions.checkArgument(parent != null,
        "parent is null, oldDir=%s", oldDir);

    final INodeDirectoryWithSnapshot newDir = new INodeDirectoryWithSnapshot(
        oldDir, latestSnapshot);
    parent.replaceChild(newDir, null);
    return newDir;
  }
  
  /** Diff list sorted by snapshot IDs, i.e. in chronological order. */
  private final List<SnapshotDiff> diffs = new ArrayList<SnapshotDiff>();

  INodeDirectoryWithSnapshot(INodeDirectory that, Snapshot s) {
    super(that);

    // add a diff for the snapshot
    addSnapshotDiff(s, that.getChildrenList(null).size());
  }

  INodeDirectoryWithSnapshot(String name, INodeDirectory dir, Snapshot s) {
    this(dir, s);
    setLocalName(name);
    setParent(dir);
  }
  
  SnapshotDiff addSnapshotDiff(Snapshot snapshot, int childrenSize) {
    final SnapshotDiff d = new SnapshotDiff(snapshot, childrenSize); 
    diffs.add(d);
    return d;
  }

  /**
   * Check if the latest snapshot diff exist.  If not, add it.
   * @return the latest snapshot diff, which is never null.
   */
  private SnapshotDiff checkAndAddLatestSnapshotDiff(Snapshot latest) {
    final SnapshotDiff last = getLastSnapshotDiff();
    if (last != null && last.snapshot.equals(latest)) {
      return last;
    }

    final int size = getChildrenList(null).size();
    final SnapshotDiff d = addSnapshotDiff(latest, size);
    if (last != null) {
      last.posteriorDiff = d;
    }
    return d;
  }
  
  Diff getLatestDiff(Snapshot latest) {
    return checkAndAddLatestSnapshotDiff(latest).diff;
  }

  /**
   * @return the diff corresponding to the snapshot.
   *         When the diff is not found, it means that the current state and
   *         the snapshot state are the same. 
   */
  SnapshotDiff getSnapshotDiff(Snapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    final int i = Collections.binarySearch(diffs, snapshot);
    if (i >= 0) {
      // exact match
      return diffs.get(i);
    } else {
      // Exact match not found means that there were no changes between
      // given snapshot and the next state so that the diff for the given
      // snapshot is not recorded.  Thus, use the next state.
      final int j = -i - 1;
      return j < diffs.size()? diffs.get(j): null;
    }
  }
  
  SnapshotDiff getLastSnapshotDiff() {
    return diffs.get(diffs.size() - 1);
  }

  @Override
  public ReadOnlyList<INode> getChildrenList(Snapshot snapshot) {
    final SnapshotDiff diff = getSnapshotDiff(snapshot);
    if (diff != null) {
      return diff.getChildrenList();
    }
    return super.getChildrenList(null);
  }

  @Override
  public INode getChild(byte[] name, Snapshot snapshot) {
    final SnapshotDiff diff = getSnapshotDiff(snapshot);
    if (diff != null) {
      return diff.getChild(name);
    }
    return super.getChild(name, null);
  }
  
  @Override
  public boolean addChild(INode inode, boolean setModTime,
      Snapshot latestSnapshot) {
    getLatestDiff(latestSnapshot).create(inode);
    return super.addChild(inode, setModTime, null);
  }

  @Override
  public INode removeChild(INode inode, Snapshot latestSnapshot) {
    getLatestDiff(latestSnapshot).delete(inode);
    return super.removeChild(inode, null);
  }

  @Override
  public INode replaceChild(INodeDirectory newChild, Snapshot latestSnapshot) {
    final INode oldChild = super.replaceChild(newChild, null);
    final Diff diff = getLatestDiff(latestSnapshot);
    diff.delete(oldChild);
    diff.create(newChild);
    return oldChild;
  }

  @Override
  public long getModificationTime(Snapshot snapshot) {
    final SnapshotDiff diff = getSnapshotDiff(snapshot);
    if (diff != null) {
      return diff.snapshotINode.getModificationTime();
    }
    return getModificationTime();
  }
}
