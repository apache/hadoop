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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ContentSummaryComputationContext {
  private FSDirectory dir = null;
  private FSNamesystem fsn = null;
  private Content.Counts counts = null;
  private long nextCountLimit = 0;
  private long limitPerRun = 0;
  private long yieldCount = 0;

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
      FSNamesystem fsn, long limitPerRun) {
    this.dir = dir;
    this.fsn = fsn;
    this.limitPerRun = limitPerRun;
    this.nextCountLimit = limitPerRun;
    this.counts = Content.Counts.newInstance();
  }

  /** Constructor for blocking computation. */
  public ContentSummaryComputationContext() {
    this(null, null, 0);
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
    long currentCount = counts.get(Content.FILE) +
        counts.get(Content.SYMLINK) +
        counts.get(Content.DIRECTORY) +
        counts.get(Content.SNAPSHOTTABLE_DIRECTORY);
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
      Thread.sleep(1);
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
  public Content.Counts getCounts() {
    return counts;
  }
}
