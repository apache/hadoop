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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * FSTreeTraverser traverse directory recursively and process files
 * in batches.
 */
@InterfaceAudience.Private
public abstract class FSTreeTraverser {


  public static final Logger LOG = LoggerFactory
      .getLogger(FSTreeTraverser.class);

  private final FSDirectory dir;

  private long readLockReportingThresholdMs;

  private Timer timer;

  public FSTreeTraverser(FSDirectory dir, Configuration conf) {
    this.dir = dir;
    this.readLockReportingThresholdMs = conf.getLong(
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY,
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT);
    timer = new Timer();
  }

  public FSDirectory getFSDirectory() {
    return dir;
  }

  /**
   * Iterate through all files directly inside parent, and recurse down
   * directories. The listing is done in batch, and can optionally start after
   * a position. The iteration of the inode tree is done in a depth-first
   * fashion. But instead of holding all {@link INodeDirectory}'s in memory
   * on the fly, only the path components to the current inode is held. This
   * is to reduce memory consumption.
   *
   * @param parent
   *          The inode id of parent directory
   * @param startId
   *          Id of the start inode.
   * @param startAfter
   *          Full path of a file the traverse should start after.
   * @param traverseInfo
   *          info which may required for processing the child's.
   * @throws IOException
   * @throws InterruptedException
   */
  protected void traverseDir(final INodeDirectory parent, final long startId,
      byte[] startAfter, final TraverseInfo traverseInfo)
      throws IOException, InterruptedException {
    List<byte[]> startAfters = new ArrayList<>();
    if (parent == null) {
      return;
    }
    INode curr = parent;
    // construct startAfters all the way up to the zone inode.
    startAfters.add(startAfter);
    while (curr.getId() != startId) {
      startAfters.add(0, curr.getLocalNameBytes());
      curr = curr.getParent();
    }
    curr = traverseDirInt(startId, parent, startAfters, traverseInfo);
    while (!startAfters.isEmpty()) {
      if (curr == null) {
        // lock was reacquired, re-resolve path.
        curr = resolvePaths(startId, startAfters);
      }
      curr = traverseDirInt(startId, curr, startAfters, traverseInfo);
    }
  }

  /**
   * Iterates the parent directory, and add direct children files to current
   * batch. If batch size meets configured threshold, current batch will be
   * submitted for the processing.
   * <p>
   * Locks could be released and reacquired when a batch submission is
   * finished.
   *
   * @param startId
   *          Id of the start inode.
   * @return The inode which was just processed, if lock is held in the entire
   *         process. Null if lock is released.
   * @throws IOException
   * @throws InterruptedException
   */
  protected INode traverseDirInt(final long startId, INode curr,
      List<byte[]> startAfters, final TraverseInfo traverseInfo)
      throws IOException, InterruptedException {
    assert dir.hasReadLock();
    assert dir.getFSNamesystem().hasReadLock();
    long lockStartTime = timer.monotonicNow();
    Preconditions.checkNotNull(curr, "Current inode can't be null");
    checkINodeReady(startId);
    final INodeDirectory parent = curr.isDirectory() ? curr.asDirectory()
        : curr.getParent();
    ReadOnlyList<INode> children = parent
        .getChildrenList(Snapshot.CURRENT_STATE_ID);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Traversing directory {}", parent.getFullPathName());
    }

    final byte[] startAfter = startAfters.get(startAfters.size() - 1);
    boolean lockReleased = false;
    for (int i = INodeDirectory.nextChild(children, startAfter); i < children
        .size(); ++i) {
      final INode inode = children.get(i);
      if (!processFileInode(inode, traverseInfo)) {
        // inode wasn't processes. Recurse down if it's a dir,
        // skip otherwise.
        if (!inode.isDirectory()) {
          continue;
        }

        if (!canTraverseDir(inode)) {
          continue;
        }
        // add 1 level to the depth-first search.
        curr = inode;
        if (!startAfters.isEmpty()) {
          startAfters.remove(startAfters.size() - 1);
          startAfters.add(curr.getLocalNameBytes());
        }
        startAfters.add(HdfsFileStatus.EMPTY_NAME);
        return lockReleased ? null : curr;
      }
      if (shouldSubmitCurrentBatch()) {
        final byte[] currentStartAfter = inode.getLocalNameBytes();
        final String parentPath = parent.getFullPathName();
        lockReleased = true;
        readUnlock();
        submitCurrentBatch(startId);
        try {
          throttle();
          checkPauseForTesting();
        } finally {
          readLock();
          lockStartTime = timer.monotonicNow();
        }
        checkINodeReady(startId);

        // Things could have changed when the lock was released.
        // Re-resolve the parent inode.
        FSPermissionChecker pc = dir.getPermissionChecker();
        INode newParent = dir
            .resolvePath(pc, parentPath, FSDirectory.DirOp.READ)
            .getLastINode();
        if (newParent == null || !newParent.equals(parent)) {
          // parent dir is deleted or recreated. We're done.
          return null;
        }
        children = parent.getChildrenList(Snapshot.CURRENT_STATE_ID);
        // -1 to counter the ++ on the for loop
        i = INodeDirectory.nextChild(children, currentStartAfter) - 1;
      }
      if ((timer.monotonicNow()
          - lockStartTime) > readLockReportingThresholdMs) {
        readUnlock();
        try {
          throttle();
        } finally {
          readLock();
          lockStartTime = timer.monotonicNow();
        }
      }
    }
    // Successfully finished this dir, adjust pointers to 1 level up, and
    // startAfter this dir.
    startAfters.remove(startAfters.size() - 1);
    if (!startAfters.isEmpty()) {
      startAfters.remove(startAfters.size() - 1);
      startAfters.add(curr.getLocalNameBytes());
    }
    curr = curr.getParent();
    return lockReleased ? null : curr;
  }

  /**
   * Resolve the cursor of traverse to an inode.
   * <p>
   * The parent of the lowest level startAfter is returned. If somewhere in the
   * middle of startAfters changed, the parent of the lowest unchanged level is
   * returned.
   *
   * @param startId
   *          Id of the start inode.
   * @param startAfters
   *          the cursor, represented by a list of path bytes.
   * @return the parent inode corresponding to the startAfters, or null if the
   *         furthest parent is deleted.
   */
  private INode resolvePaths(final long startId, List<byte[]> startAfters)
      throws IOException {
    // If the readlock was reacquired, we need to resolve the paths again
    // in case things have changed. If our cursor file/dir is changed,
    // continue from the next one.
    INode zoneNode = dir.getInode(startId);
    if (zoneNode == null) {
      throw new FileNotFoundException("Zone " + startId + " is deleted.");
    }
    INodeDirectory parent = zoneNode.asDirectory();
    for (int i = 0; i < startAfters.size(); ++i) {
      if (i == startAfters.size() - 1) {
        // last startAfter does not need to be resolved, since search for
        // nextChild will cover that automatically.
        break;
      }
      INode curr = parent.getChild(startAfters.get(i),
          Snapshot.CURRENT_STATE_ID);
      if (curr == null) {
        // inode at this level has changed. Update startAfters to point to
        // the next dir at the parent level (and dropping any startAfters
        // at lower levels).
        for (; i < startAfters.size(); ++i) {
          startAfters.remove(startAfters.size() - 1);
        }
        break;
      }
      parent = curr.asDirectory();
    }
    return parent;
  }

  protected void readLock() {
    dir.getFSNamesystem().readLock();
    dir.readLock();
  }

  protected void readUnlock() {
    dir.readUnlock();
    dir.getFSNamesystem().readUnlock("FSTreeTraverser");
  }


  protected abstract void checkPauseForTesting() throws InterruptedException;

  /**
   * Process an Inode. Add to current batch if it's a file, no-op otherwise.
   *
   * @param inode
   *          the inode
   * @return true if inode is added to currentBatch and should be process for
   *         next operation. false otherwise: could be inode is not a file.
   * @throws IOException
   * @throws InterruptedException
   */
  protected abstract boolean processFileInode(INode inode,
      TraverseInfo traverseInfo) throws IOException, InterruptedException;

  /**
   * Check whether current batch can be submitted for the processing.
   *
   * @return true if batch size meets the condition, otherwise false.
   */
  protected abstract boolean shouldSubmitCurrentBatch();

  /**
   * Check whether inode is ready for traverse. Throws IOE if it's not.
   *
   * @param startId
   *          Id of the start inode.
   * @throws IOException
   */
  protected abstract void checkINodeReady(long startId) throws IOException;

  /**
   * Submit the current batch for processing.
   *
   * @param startId
   *          Id of the start inode.
   * @throws IOException
   * @throws InterruptedException
   */
  protected abstract void submitCurrentBatch(Long startId)
      throws IOException, InterruptedException;

  /**
   * Throttles the FSTreeTraverser.
   *
   * @throws InterruptedException
   */
  protected abstract void throttle() throws InterruptedException;

  /**
   * Check whether dir is traversable or not.
   *
   * @param inode
   *          Dir inode
   * @return true if dir is traversable otherwise false.
   * @throws IOException
   */
  protected abstract boolean canTraverseDir(INode inode) throws IOException;

  /**
   * Class will represent the additional info required for traverse.
   */
  public static class TraverseInfo {

  }
}
