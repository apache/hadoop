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

import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.Quota;

/**
 * A list of snapshot diffs for storing snapshot data.
 *
 * @param <N> The {@link INode} type.
 * @param <D> The diff type, which must extend {@link AbstractINodeDiff}.
 */
abstract class AbstractINodeDiffList<N extends INode,
                                     A extends INodeAttributes,
                                     D extends AbstractINodeDiff<N, A, D>> 
    implements Iterable<D> {
  /** Diff list sorted by snapshot IDs, i.e. in chronological order. */
  private final List<D> diffs = new ArrayList<D>();

  /** @return this list as a unmodifiable {@link List}. */
  public final List<D> asList() {
    return Collections.unmodifiableList(diffs);
  }
  
  /** Get the size of the list and then clear it. */
  public void clear() {
    diffs.clear();
  }

  /** @return an {@link AbstractINodeDiff}. */
  abstract D createDiff(Snapshot snapshot, N currentINode);

  /** @return a snapshot copy of the current inode. */  
  abstract A createSnapshotCopy(N currentINode);

  /**
   * Delete a snapshot. The synchronization of the diff list will be done 
   * outside. If the diff to remove is not the first one in the diff list, we 
   * need to combine the diff with its previous one.
   * 
   * @param snapshot The snapshot to be deleted
   * @param prior The snapshot taken before the to-be-deleted snapshot
   * @param collectedBlocks Used to collect information for blocksMap update
   * @return delta in namespace. 
   */
  public final Quota.Counts deleteSnapshotDiff(final Snapshot snapshot,
      Snapshot prior, final N currentINode,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, boolean countDiffChange) 
      throws QuotaExceededException {
    int snapshotIndex = Collections.binarySearch(diffs, snapshot.getId());
    
    Quota.Counts counts = Quota.Counts.newInstance();
    D removed = null;
    if (snapshotIndex == 0) {
      if (prior != null) {
        // set the snapshot to latestBefore
        diffs.get(snapshotIndex).setSnapshot(prior);
      } else {
        removed = diffs.remove(0);
        if (countDiffChange) {
          counts.add(Quota.NAMESPACE, 1);
        } else {
          // the currentINode must be a descendant of a WithName node, which set
          // countDiffChange to false. In that case we should count in the diff
          // change when updating the quota usage in the current tree
          currentINode.addSpaceConsumed(-1, 0, false);
        }
        counts.add(removed.destroyDiffAndCollectBlocks(currentINode,
            collectedBlocks, removedINodes));
      }
    } else if (snapshotIndex > 0) {
      final AbstractINodeDiff<N, A, D> previous = diffs.get(snapshotIndex - 1);
      if (!previous.getSnapshot().equals(prior)) {
        diffs.get(snapshotIndex).setSnapshot(prior);
      } else {
        // combine the to-be-removed diff with its previous diff
        removed = diffs.remove(snapshotIndex);
        if (countDiffChange) {
          counts.add(Quota.NAMESPACE, 1);
        } else {
          currentINode.addSpaceConsumed(-1, 0, false);
        }
        if (previous.snapshotINode == null) {
          previous.snapshotINode = removed.snapshotINode;
        }

        counts.add(previous.combinePosteriorAndCollectBlocks(
            currentINode, removed, collectedBlocks, removedINodes));
        previous.setPosterior(removed.getPosterior());
        removed.setPosterior(null);
      }
    }
    return counts;
  }

  /** Add an {@link AbstractINodeDiff} for the given snapshot. */
  final D addDiff(Snapshot latest, N currentINode)
      throws QuotaExceededException {
    currentINode.addSpaceConsumed(1, 0, true);
    return addLast(createDiff(latest, currentINode));
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
  public final D getLast() {
    final int n = diffs.size();
    return n == 0? null: diffs.get(n - 1);
  }

  /** @return the last snapshot. */
  public final Snapshot getLastSnapshot() {
    final AbstractINodeDiff<N, A, D> last = getLast();
    return last == null? null: last.getSnapshot();
  }
  
  /**
   * Find the latest snapshot before a given snapshot.
   * @param anchorId The returned snapshot's id must be <= or < this given 
   *                 snapshot id.
   * @param exclusive True means the returned snapshot's id must be < the given
   *                  id, otherwise <=.
   * @return The latest snapshot before the given snapshot.
   */
  private final Snapshot getPrior(int anchorId, boolean exclusive) {
    if (anchorId == Snapshot.INVALID_ID) {
      return getLastSnapshot();
    }
    final int i = Collections.binarySearch(diffs, anchorId);
    if (exclusive) { // must be the one before
      if (i == -1 || i == 0) {
        return null;
      } else {
        int priorIndex = i > 0 ? i - 1 : -i - 2;
        return diffs.get(priorIndex).getSnapshot();
      }
    } else { // the one, or the one before if not existing
      if (i >= 0) {
        return diffs.get(i).getSnapshot();
      } else if (i < -1) {
        return diffs.get(-i - 2).getSnapshot();
      } else { // i == -1
        return null;
      }
    }
  }
  
  public final Snapshot getPrior(int snapshotId) {
    return getPrior(snapshotId, false);
  }
  
  /**
   * Update the prior snapshot.
   */
  final Snapshot updatePrior(Snapshot snapshot, Snapshot prior) {
    int id = snapshot == null ? Snapshot.INVALID_ID : snapshot.getId();
    Snapshot s = getPrior(id, true);
    if (s != null && 
        (prior == null || Snapshot.ID_COMPARATOR.compare(s, prior) > 0)) {
      return s;
    }
    return prior;
  }

  /**
   * @return the diff corresponding to the given snapshot.
   *         When the diff is null, it means that the current state and
   *         the corresponding snapshot state are the same. 
   */
  public final D getDiff(Snapshot snapshot) {
    return getDiffById(snapshot == null ? 
        Snapshot.INVALID_ID : snapshot.getId());
  }
  
  private final D getDiffById(final int snapshotId) {
    if (snapshotId == Snapshot.INVALID_ID) {
      return null;
    }
    final int i = Collections.binarySearch(diffs, snapshotId);
    if (i >= 0) {
      // exact match
      return diffs.get(i);
    } else {
      // Exact match not found means that there were no changes between
      // given snapshot and the next state so that the diff for the given
      // snapshot was not recorded. Thus, return the next state.
      final int j = -i - 1;
      return j < diffs.size()? diffs.get(j): null;
    }
  }
  
  /**
   * Search for the snapshot whose id is 1) no less than the given id, 
   * and 2) most close to the given id.
   */
  public final Snapshot getSnapshotById(final int snapshotId) {
    D diff = getDiffById(snapshotId);
    return diff == null ? null : diff.getSnapshot();
  }
  
  /**
   * Check if changes have happened between two snapshots.
   * @param earlier The snapshot taken earlier
   * @param later The snapshot taken later
   * @return Whether or not modifications (including diretory/file metadata
   *         change, file creation/deletion under the directory) have happened
   *         between snapshots.
   */
  final boolean changedBetweenSnapshots(Snapshot earlier, Snapshot later) {
    final int size = diffs.size();
    int earlierDiffIndex = Collections.binarySearch(diffs, earlier.getId());
    if (-earlierDiffIndex - 1 == size) {
      // if the earlierSnapshot is after the latest SnapshotDiff stored in
      // diffs, no modification happened after the earlierSnapshot
      return false;
    }
    if (later != null) {
      int laterDiffIndex = Collections.binarySearch(diffs, later.getId());
      if (laterDiffIndex == -1 || laterDiffIndex == 0) {
        // if the laterSnapshot is the earliest SnapshotDiff stored in diffs, or
        // before it, no modification happened before the laterSnapshot
        return false;
      }
    }
    return true;
  }

  /**
   * @return the inode corresponding to the given snapshot.
   *         Note that the current inode is returned if there is no change
   *         between the given snapshot and the current state. 
   */
  A getSnapshotINode(final Snapshot snapshot, final A currentINode) {
    final D diff = getDiff(snapshot);
    final A inode = diff == null? null: diff.getSnapshotINode();
    return inode == null? currentINode: inode;
  }

  /**
   * Check if the latest snapshot diff exists.  If not, add it.
   * @return the latest snapshot diff, which is never null.
   */
  final D checkAndAddLatestSnapshotDiff(Snapshot latest, N currentINode)
      throws QuotaExceededException {
    final D last = getLast();
    if (last != null
        && Snapshot.ID_COMPARATOR.compare(last.getSnapshot(), latest) >= 0) {
      return last;
    } else {
      try {
        return addDiff(latest, currentINode);
      } catch(NSQuotaExceededException e) {
        e.setMessagePrefix("Failed to record modification for snapshot");
        throw e;
      }
    }
  }

  /** Save the snapshot copy to the latest snapshot. */
  public void saveSelf2Snapshot(Snapshot latest, N currentINode, A snapshotCopy)
      throws QuotaExceededException {
    if (latest != null) {
      D diff = checkAndAddLatestSnapshotDiff(latest, currentINode);
      if (diff.snapshotINode == null) {
        if (snapshotCopy == null) {
          snapshotCopy = createSnapshotCopy(currentINode);
        }
        diff.saveSnapshotCopy(snapshotCopy, currentINode);
      }
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