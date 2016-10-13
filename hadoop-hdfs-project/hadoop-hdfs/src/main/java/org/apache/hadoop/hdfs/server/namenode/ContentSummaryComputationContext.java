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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.util.HashSet;
import java.util.Set;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ContentSummaryComputationContext {
  private FSDirectory dir = null;
  private FSNamesystem fsn = null;
  private BlockStoragePolicySuite bsps = null;
  private ContentCounts counts = null;
  private ContentCounts snapshotCounts = null;
  private long nextCountLimit = 0;
  private long limitPerRun = 0;
  private long yieldCount = 0;
  private long sleepMilliSec = 0;
  private int sleepNanoSec = 0;
  private Set<INode> includedNodes = new HashSet<>();
  private Set<INode> deletedSnapshottedNodes = new HashSet<>();

  /**
   * Constructor
   *
   * @param dir The FSDirectory instance
   * @param fsn The FSNamesystem instance
   * @param limitPerRun allowed number of operations in one
   *        locking period. 0 or a negative number means
   *        no limit (i.e. no yielding)
   */
  public ContentSummaryComputationContext(FSDirectory dir,
      FSNamesystem fsn, long limitPerRun, long sleepMicroSec) {
    this.dir = dir;
    this.fsn = fsn;
    this.limitPerRun = limitPerRun;
    this.nextCountLimit = limitPerRun;
    setCounts(new ContentCounts.Builder().build());
    setSnapshotCounts(new ContentCounts.Builder().build());
    this.sleepMilliSec = sleepMicroSec/1000;
    this.sleepNanoSec = (int)((sleepMicroSec%1000)*1000);
  }

  /** Constructor for blocking computation. */
  public ContentSummaryComputationContext(BlockStoragePolicySuite bsps) {
    this(null, null, 0, 1000);
    this.bsps = bsps;
  }

  /** Return current yield count */
  public long getYieldCount() {
    return yieldCount;
  }

  /**
   * Relinquish locks held during computation for a short while
   * and reacquire them. This will give other threads a chance
   * to acquire the contended locks and run.
   *
   * @return true if locks were released and reacquired.
   */
  public boolean yield() {
    // Are we set up to do this?
    if (limitPerRun <= 0 || dir == null || fsn == null) {
      return false;
    }

    // Have we reached the limit?
    ContentCounts counts = getCounts();
    long currentCount = counts.getFileCount() +
        counts.getSymlinkCount() +
        counts.getDirectoryCount() +
        counts.getSnapshotableDirectoryCount();
    if (currentCount <= nextCountLimit) {
      return false;
    }

    // Update the next limit
    nextCountLimit = currentCount + limitPerRun;

    boolean hadDirReadLock = dir.hasReadLock();
    boolean hadDirWriteLock = dir.hasWriteLock();
    boolean hadFsnReadLock = fsn.hasReadLock();
    boolean hadFsnWriteLock = fsn.hasWriteLock();

    // sanity check.
    if (!hadDirReadLock || !hadFsnReadLock || hadDirWriteLock ||
        hadFsnWriteLock || dir.getReadHoldCount() != 1 ||
        fsn.getReadHoldCount() != 1) {
      // cannot relinquish
      return false;
    }

    // unlock
    dir.readUnlock();
    fsn.readUnlock();

    try {
      Thread.sleep(sleepMilliSec, sleepNanoSec);
    } catch (InterruptedException ie) {
    } finally {
      // reacquire
      fsn.readLock();
      dir.readLock();
    }
    yieldCount++;
    return true;
  }

  /** Get the content counts */
  public synchronized ContentCounts getCounts() {
    return counts;
  }

  private synchronized void setCounts(ContentCounts counts) {
    this.counts = counts;
  }

  public ContentCounts getSnapshotCounts() {
    return snapshotCounts;
  }

  private void setSnapshotCounts(ContentCounts snapshotCounts) {
    this.snapshotCounts = snapshotCounts;
  }

  public BlockStoragePolicySuite getBlockStoragePolicySuite() {
    Preconditions.checkState((bsps != null || fsn != null),
        "BlockStoragePolicySuite must be either initialized or available via" +
            " FSNameSystem");
    return (bsps != null) ? bsps:
        fsn.getBlockManager().getStoragePolicySuite();
  }

  /**
   * If the node is an INodeReference, resolves it to the actual inode.
   * Snapshot diffs represent renamed / moved files as different
   * INodeReferences, but the underlying INode it refers to is consistent.
   *
   * @param node
   * @return The referred INode if there is one, else returns the input
   * unmodified.
   */
  private INode resolveINodeReference(INode node) {
    if (node.isReference() && node instanceof INodeReference) {
      return ((INodeReference)node).getReferredINode();
    }
    return node;
  }

  /**
   * Reports that a node is about to be included in this summary. Can be used
   * either to simply report that a node has been including, or check whether
   * a node has already been included.
   *
   * @param node
   * @return true if node has already been included
   */
  public boolean nodeIncluded(INode node) {
    INode resolvedNode = resolveINodeReference(node);
    synchronized (includedNodes) {
      if (!includedNodes.contains(resolvedNode)) {
        includedNodes.add(resolvedNode);
        return false;
      }
    }
    return true;
  }

  /**
   * Schedules a node that is listed as DELETED in a snapshot's diff to be
   * included in the summary at the end of computation. See
   * {@link #tallyDeletedSnapshottedINodes()} for more context.
   *
   * @param node
   */
  public void reportDeletedSnapshottedNode(INode node) {
    deletedSnapshottedNodes.add(node);
  }

  /**
   * Finalizes the computation by including all nodes that were reported as
   * deleted by a snapshot but have not been already including due to other
   * references.
   * <p>
   * Nodes that get renamed are listed in the snapshot's diff as both DELETED
   * under the old name and CREATED under the new name. The computation
   * relies on nodes to report themselves as being included (via
   * {@link #nodeIncluded(INode)} as the only reliable way to determine which
   * nodes were renamed within the tree being summarized and which were
   * removed (either by deletion or being renamed outside of the tree).
   */
  public synchronized void tallyDeletedSnapshottedINodes() {
    /* Temporarily create a new counts object so these results can then be
    added to both counts and snapshotCounts */
    ContentCounts originalCounts = getCounts();
    setCounts(new ContentCounts.Builder().build());
    for (INode node : deletedSnapshottedNodes) {
      if (!nodeIncluded(node)) {
        node.computeContentSummary(Snapshot.CURRENT_STATE_ID, this);
      }
    }
    originalCounts.addContents(getCounts());
    snapshotCounts.addContents(getCounts());
    setCounts(originalCounts);
  }
}
