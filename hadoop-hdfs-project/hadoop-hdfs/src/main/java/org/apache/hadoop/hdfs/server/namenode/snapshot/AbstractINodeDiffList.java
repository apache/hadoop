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

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;

/**
 * A list of snapshot diffs for storing snapshot data.
 *
 * @param <N> The {@link INode} type.
 * @param <D> The diff type, which must extend {@link AbstractINodeDiff}.
 */
abstract class AbstractINodeDiffList<N extends INode,
                                     D extends AbstractINodeDiff<N, D>> 
    implements Iterable<D> {
  private AbstractINodeDiff.Factory<N, D> factory;

  /** Diff list sorted by snapshot IDs, i.e. in chronological order. */
  private final List<D> diffs = new ArrayList<D>();

  void setFactory(AbstractINodeDiff.Factory<N, D> factory) {
    this.factory = factory;
  }

  /** @return this list as a unmodifiable {@link List}. */
  final List<D> asList() {
    return Collections.unmodifiableList(diffs);
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
  final D deleteSnapshotDiff(final Snapshot snapshot, final N currentINode,
      final BlocksMapUpdateInfo collectedBlocks) {
    int snapshotIndex = Collections.binarySearch(diffs, snapshot);
    if (snapshotIndex < 0) {
      return null;
    } else {
      final D removed = diffs.remove(snapshotIndex);
      if (snapshotIndex == 0) {
        if (removed.snapshotINode != null) {
          removed.snapshotINode.clearReferences();
        }
      } else {
        // combine the to-be-removed diff with its previous diff
        final AbstractINodeDiff<N, D> previous = diffs.get(snapshotIndex - 1);
        if (previous.snapshotINode == null) {
          previous.snapshotINode = removed.snapshotINode;
        } else if (removed.snapshotINode != null) {
          removed.snapshotINode.clearReferences();
        }
        previous.combinePosteriorAndCollectBlocks(currentINode, removed,
            collectedBlocks);
        previous.setPosterior(removed.getPosterior());
      }
      removed.setPosterior(null);
      return removed;
    }
  }

  /** Add an {@link AbstractINodeDiff} for the given snapshot. */
  final D addDiff(Snapshot latest, N currentINode) {
    return addLast(factory.createDiff(latest, currentINode));
  }

  /** Append the diff at the end of the list. */
  private final D addLast(D diff) {
    final D last = getLast();
    diffs.add(diff);
    if (last != null) {
      last.setPosterior(diff);
    }
    return diff;
  }
  
  /** Add the diff to the beginning of the list. */
  final void addFirst(D diff) {
    final D first = diffs.isEmpty()? null: diffs.get(0);
    diffs.add(0, diff);
    diff.setPosterior(first);
  }

  /** @return the last diff. */
  final D getLast() {
    final int n = diffs.size();
    return n == 0? null: diffs.get(n - 1);
  }

  /** @return the last snapshot. */
  final Snapshot getLastSnapshot() {
    final AbstractINodeDiff<N, D> last = getLast();
    return last == null? null: last.getSnapshot();
  }

  /**
   * @return the diff corresponding to the given snapshot.
   *         When the diff is null, it means that the current state and
   *         the corresponding snapshot state are the same. 
   */
  final D getDiff(Snapshot snapshot) {
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

  /**
   * @return the inode corresponding to the given snapshot.
   *         Note that the current inode is returned if there is no change
   *         between the given snapshot and the current state. 
   */
  N getSnapshotINode(final Snapshot snapshot, final N currentINode) {
    final D diff = getDiff(snapshot);
    final N inode = diff == null? null: diff.getSnapshotINode();
    return inode == null? currentINode: inode;
  }

  /**
   * Check if the latest snapshot diff exists.  If not, add it.
   * @return the latest snapshot diff, which is never null.
   */
  final D checkAndAddLatestSnapshotDiff(Snapshot latest, N currentINode) {
    final D last = getLast();
    return last != null && last.snapshot.equals(latest)? last
        : addDiff(latest, currentINode);
  }

  /** Save the snapshot copy to the latest snapshot. */
  void saveSelf2Snapshot(Snapshot latest, N currentINode, N snapshotCopy) {
    if (latest != null) {
      checkAndAddLatestSnapshotDiff(latest, currentINode).saveSnapshotCopy(
          snapshotCopy, factory, currentINode);
    }
  }
  
  @Override
  public Iterator<D> iterator() {
    return diffs.iterator();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + diffs;
  }
}