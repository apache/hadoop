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

    /** The ID (e.g. snapshot ID) of this object. */
    final int id;
    /** c-list: inode(s) created in current. */
    private List<INode> created;
    /** d-list: inode(s) deleted from current. */
    private List<INode> deleted;

    Diff(int id) {
      this.id = id;
    }

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
      return getClass().getSimpleName() + "_" + id
          + ":\n  created=" + toString(created)
          + "\n  deleted=" + toString(deleted);
    }
  }

  INodeDirectoryWithSnapshot(String name, INodeDirectory dir) {
    super(name, dir.getPermissionStatus());
    parent = dir;
  }
}