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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;

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

  /** Diff list sorted by snapshot IDs, i.e. in chronological order.
    * Created lazily to avoid wasting memory by empty lists. */
  private DiffList<D> diffs;

  /** @return this list as a unmodifiable {@link List}. */
  public final DiffList<D> asList() {
    return diffs != null ?
        DiffList.unmodifiableList(diffs) : DiffList.emptyList();
  }

  public boolean isEmpty() {
    return diffs == null || diffs.isEmpty();
  }
  
  /** Clear the list. */
  public void clear() {
    diffs = null;
  }

  /** @return an {@link AbstractINodeDiff}. */
  abstract D createDiff(int snapshotId, N currentINode);

  /** @return a snapshot copy of the current inode. */  
  abstract A createSnapshotCopy(N currentINode);

  /**
   * Delete a snapshot. The synchronization of the diff list will be done 
   * outside. If the diff to remove is not the first one in the diff list, we 
   * need to combine the diff with its previous one.
   * 
   * @param reclaimContext blocks and inodes that need to be reclaimed
   * @param snapshot The id of the snapshot to be deleted
   * @param prior The id of the snapshot taken before the to-be-deleted snapshot
   * @param currentINode the inode where the snapshot diff is deleted
   */
  public final void deleteSnapshotDiff(INode.ReclaimContext reclaimContext,
      final int snapshot, final int prior, final N currentINode) {
    if (diffs == null) {
      return;
    }
    int snapshotIndex = diffs.binarySearch(snapshot);

    D removed;
    if (snapshotIndex == 0) {
      if (prior != Snapshot.NO_SNAPSHOT_ID) { // there is still snapshot before
        // set the snapshot to latestBefore
        diffs.get(snapshotIndex).setSnapshotId(prior);
      } else { // there is no snapshot before
        removed = diffs.remove(0);
        if (diffs.isEmpty()) {
          diffs = null;
        }
        removed.destroyDiffAndCollectBlocks(reclaimContext, currentINode);
      }
    } else if (snapshotIndex > 0) {
      final AbstractINodeDiff<N, A, D> previous = diffs.get(snapshotIndex - 1);
      if (previous.getSnapshotId() != prior) {
        diffs.get(snapshotIndex).setSnapshotId(prior);
      } else {
        // combine the to-be-removed diff with its previous diff
        removed = diffs.remove(snapshotIndex);
        if (previous.snapshotINode == null) {
          previous.snapshotINode = removed.snapshotINode;
        }

        previous.combinePosteriorAndCollectBlocks(reclaimContext, currentINode,
            removed);
        previous.setPosterior(removed.getPosterior());
        removed.setPosterior(null);
      }
    }
  }

  /** Add an {@link AbstractINodeDiff} for the given snapshot. */
  final D addDiff(int latestSnapshotId, N currentINode) {
    return addLast(createDiff(latestSnapshotId, currentINode));
  }

  /** Append the diff at the end of the list. */
  private D addLast(D diff) {
    createDiffsIfNeeded();
    final D last = getLast();
    diffs.addLast(diff);
    if (last != null) {
      last.setPosterior(diff);
    }
    return diff;
  }
  
  /** Add the diff to the beginning of the list. */
  final void addFirst(D diff) {
    createDiffsIfNeeded();
    final D first = diffs.isEmpty()? null : diffs.get(0);
    diffs.addFirst(diff);
    diff.setPosterior(first);
  }

  /** @return the last diff. */
  public final D getLast() {
    if (diffs == null) {
      return null;
    }
    int n = diffs.size();
    return n == 0 ? null : diffs.get(n - 1);
  }

  DiffList<D> newDiffs() {
    return new DiffListByArrayList<>(
        INodeDirectory.DEFAULT_FILES_PER_DIRECTORY);
  }

  private void createDiffsIfNeeded() {
    if (diffs == null) {
      diffs = newDiffs();
    }
  }

  /** @return the id of the last snapshot. */
  public final int getLastSnapshotId() {
    final AbstractINodeDiff<N, A, D> last = getLast();
    return last == null ? Snapshot.CURRENT_STATE_ID : last.getSnapshotId();
  }
  
  /**
   * Find the latest snapshot before a given snapshot.
   * @param anchorId The returned snapshot's id must be &lt;= or &lt; this
   *                 given snapshot id.
   * @param exclusive True means the returned snapshot's id must be &lt; the
   *                  given id, otherwise &lt;=.
   * @return The id of the latest snapshot before the given snapshot.
   */
  public final int getPrior(int anchorId, boolean exclusive) {
    if (diffs == null) {
      return Snapshot.NO_SNAPSHOT_ID;
    }
    if (anchorId == Snapshot.CURRENT_STATE_ID) {
      int last = getLastSnapshotId();
      if (exclusive && last == anchorId) {
        return Snapshot.NO_SNAPSHOT_ID;
      }
      return last;
    }
    final int i = diffs.binarySearch(anchorId);
    if (exclusive) { // must be the one before
      if (i == -1 || i == 0) {
        return Snapshot.NO_SNAPSHOT_ID;
      } else {
        int priorIndex = i > 0 ? i - 1 : -i - 2;
        return diffs.get(priorIndex).getSnapshotId();
      }
    } else { // the one, or the one before if not existing
      if (i >= 0) {
        return diffs.get(i).getSnapshotId();
      } else if (i < -1) {
        return diffs.get(-i - 2).getSnapshotId();
      } else { // i == -1
        return Snapshot.NO_SNAPSHOT_ID;
      }
    }
  }
  
  public final int getPrior(int snapshotId) {
    return getPrior(snapshotId, false);
  }
  
  /**
   * Update the prior snapshot.
   */
  final int updatePrior(int snapshot, int prior) {
    int p = getPrior(snapshot, true);
    if (p != Snapshot.CURRENT_STATE_ID
        && Snapshot.ID_INTEGER_COMPARATOR.compare(p, prior) > 0) {
      return p;
    }
    return prior;
  }
  
  public final D getDiffById(final int snapshotId) {
    if (snapshotId == Snapshot.CURRENT_STATE_ID || diffs == null) {
      return null;
    }
    final int i = diffs.binarySearch(snapshotId);
    if (i >= 0) {
      // exact match
      return diffs.get(i);
    } else {
      // Exact match not found means that there were no changes between
      // given snapshot and the next state so that the diff for the given
      // snapshot was not recorded. Thus, return the next state.
      final int j = -i - 1;
      return j < diffs.size() ? diffs.get(j) : null;
    }
  }
  
  /**
   * Search for the snapshot whose id is 1) no less than the given id, 
   * and 2) most close to the given id.
   */
  public final int getSnapshotById(final int snapshotId) {
    D diff = getDiffById(snapshotId);
    return diff == null ? Snapshot.CURRENT_STATE_ID : diff.getSnapshotId();
  }

  public final int getDiffIndexById(final int snapshotId) {
    int diffIndex = diffs.binarySearch(snapshotId);
    diffIndex = diffIndex < 0 ? (-diffIndex - 1) : diffIndex;
    return diffIndex;
  }

  final int[] changedBetweenSnapshots(Snapshot from, Snapshot to) {
    if (diffs == null) {
      return null;
    }
    Snapshot earlier = from;
    Snapshot later = to;
    if (Snapshot.ID_COMPARATOR.compare(from, to) > 0) {
      earlier = to;
      later = from;
    }

    final int size = diffs.size();
    int earlierDiffIndex = getDiffIndexById(earlier.getId());
    int laterDiffIndex = later == null ? size
        : getDiffIndexById(later.getId());
    if (earlierDiffIndex == size) {
      // if the earlierSnapshot is after the latest SnapshotDiff stored in
      // diffs, no modification happened after the earlierSnapshot
      return null;
    }
    if (laterDiffIndex == -1 || laterDiffIndex == 0) {
      // if the laterSnapshot is the earliest SnapshotDiff stored in diffs, or
      // before it, no modification happened before the laterSnapshot
      return null;
    }
    return new int[]{earlierDiffIndex, laterDiffIndex};
  }

  /**
   * @return the inode corresponding to the given snapshot.
   *         Note that the current inode is returned if there is no change
   *         between the given snapshot and the current state. 
   */
  public A getSnapshotINode(final int snapshotId, final A currentINode) {
    final D diff = getDiffById(snapshotId);
    final A inode = diff == null? null: diff.getSnapshotINode();
    return inode == null? currentINode: inode;
  }

  /**
   * Check if the latest snapshot diff exists.  If not, add it.
   * @return the latest snapshot diff, which is never null.
   */
  final D checkAndAddLatestSnapshotDiff(int latestSnapshotId, N currentINode) {
    final D last = getLast();
    return (last != null && Snapshot.ID_INTEGER_COMPARATOR
        .compare(last.getSnapshotId(), latestSnapshotId) >= 0) ?
        last : addDiff(latestSnapshotId, currentINode);
  }

  /** Save the snapshot copy to the latest snapshot. */
  public D saveSelf2Snapshot(int latestSnapshotId, N currentINode,
      A snapshotCopy) {
    D diff = null;
    if (latestSnapshotId != Snapshot.CURRENT_STATE_ID) {
      diff = checkAndAddLatestSnapshotDiff(latestSnapshotId, currentINode);
      if (diff.snapshotINode == null) {
        if (snapshotCopy == null) {
          snapshotCopy = createSnapshotCopy(currentINode);
        }
        diff.saveSnapshotCopy(snapshotCopy);
      }
    }
    return diff;
  }

  @Override
  public Iterator<D> iterator() {
    return diffs != null ? diffs.iterator() : Collections.emptyIterator();
  }

  @Override
  public String toString() {
    if (diffs != null) {
      final StringBuilder b =
          new StringBuilder(getClass().getSimpleName()).append("@")
              .append(Integer.toHexString(hashCode())).append(": ");
      b.append("[");
      for (D d : diffs) {
        b.append(d).append(", ");
      }
      b.setLength(b.length() - 2);
      b.append("]");
      return b.toString();
    } else {
      return "";
    }
  }
}
