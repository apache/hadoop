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
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.AccessControlException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_ERASURECODING_POLICY;

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

  public static final String REPLICATED = "Replicated";
  public static final Logger LOG = LoggerFactory
      .getLogger(ContentSummaryComputationContext.class);

  private FSPermissionChecker pc;
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
    this(dir, fsn, limitPerRun, sleepMicroSec, null);
  }

  public ContentSummaryComputationContext(FSDirectory dir,
      FSNamesystem fsn, long limitPerRun, long sleepMicroSec,
      FSPermissionChecker pc) {
    this.dir = dir;
    this.fsn = fsn;
    this.limitPerRun = limitPerRun;
    this.nextCountLimit = limitPerRun;
    this.counts = new ContentCounts.Builder().build();
    this.snapshotCounts = new ContentCounts.Builder().build();
    this.sleepMilliSec = sleepMicroSec/1000;
    this.sleepNanoSec = (int)((sleepMicroSec%1000)*1000);
    this.pc = pc;
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
    fsn.readUnlock("contentSummary");

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
  public ContentCounts getCounts() {
    return counts;
  }

  public ContentCounts getSnapshotCounts() {
    return snapshotCounts;
  }

  public BlockStoragePolicySuite getBlockStoragePolicySuite() {
    Preconditions.checkState((bsps != null || fsn != null),
        "BlockStoragePolicySuite must be either initialized or available via" +
            " FSNameSystem");
    return (bsps != null) ? bsps:
        fsn.getBlockManager().getStoragePolicySuite();
  }

  /** Get the erasure coding policy. */
  public String getErasureCodingPolicyName(INode inode) {
    if (inode.isFile()) {
      INodeFile iNodeFile = inode.asFile();
      if (iNodeFile.isStriped()) {
        byte ecPolicyId = iNodeFile.getErasureCodingPolicyID();
        return fsn.getErasureCodingPolicyManager()
            .getByID(ecPolicyId).getName();
      } else {
        return REPLICATED;
      }
    }
    if (inode.isSymlink()) {
      return "";
    }
    try {
      final XAttrFeature xaf = inode.getXAttrFeature();
      if (xaf != null) {
        XAttr xattr = xaf.getXAttr(XATTR_ERASURECODING_POLICY);
        if (xattr != null) {
          ByteArrayInputStream bins =
              new ByteArrayInputStream(xattr.getValue());
          DataInputStream din = new DataInputStream(bins);
          String ecPolicyName = WritableUtils.readString(din);
          return dir.getFSNamesystem()
              .getErasureCodingPolicyManager()
              .getEnabledPolicyByName(ecPolicyName)
              .getName();
        }
      } else if (inode.getParent() != null) {
          return getErasureCodingPolicyName(inode.getParent());
      }
    } catch (IOException ioe) {
      LOG.warn("Encountered error getting ec policy for "
          + inode.getFullPathName(), ioe);
      return "";
    }
    return "";
  }

  void checkPermission(INodeDirectory inode, int snapshotId, FsAction access)
      throws AccessControlException {
    if (dir != null && dir.isPermissionEnabled()
        && pc != null && !pc.isSuperUser()) {
      pc.checkPermission(inode, snapshotId, access);
    }
  }
}
