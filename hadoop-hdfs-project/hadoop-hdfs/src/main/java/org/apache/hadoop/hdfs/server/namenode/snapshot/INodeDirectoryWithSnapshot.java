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
import java.util.List;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;

import com.google.common.base.Preconditions;

/** The directory with snapshots. */
public class INodeDirectoryWithSnapshot extends INodeDirectoryWithQuota {
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
        // inode is already in c-list,
        previous = created.set(c, newinode);
      } else {
        d = search(deleted, oldinode);
        if (d < 0) {
          // neither in c-list nor d-list
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

  public INodeDirectoryWithSnapshot(INodeDirectory that, boolean adopt) {
    super(that, adopt, that.getNsQuota(), that.getDsQuota());
  }
}
