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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.FSMultiRootTreeWalk;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.FSTreeWalk;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.TreePath;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.TreeWalk;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * File system mount operations.
 */
class FSMountAttrOp {

  /**
   * Add the entries of a mount point into the filesystem; the FSNameSystem
   * write lock is obtained during this operation but not held more than
   * "dfs.namenode.mount.lock.duration" configuration. If more than
   * "dfs.namenode.mount.lock.duration" is required to complete this
   * operation, the lock is released and re-obtained. This method is only
   * applicable to READONLY mount.
   * @param fsn FSNamesystem.
   * @param conf Configuration.
   * @param opName Name of the operation.
   * @param remotePaths List of remote paths.
   * @throws IOException If we cannot mount.
   */
  static void addRemotePaths(FSNamesystem fsn, Configuration conf,
      String opName, List<TreePath> remotePaths) throws IOException {
    // Add files and folders into the mount point
    long lockReleasePeriod = conf.getTimeDuration(
        DFSConfigKeys.DFS_NAMENODE_MOUNT_LOCK_DURATION_MAX,
        DFSConfigKeys.DFS_NAMENODE_MOUNT_LOCK_DURATION_MAX_DEFAULT,
        TimeUnit.MILLISECONDS);
    fsn.writeLock();
    long lastLock = Time.monotonicNow();
    // TODO handle edits if the primary NN fails after adding the MountOp
    try (MountEditLogWriter w = new MountEditLogWriter(fsn, conf)) {
      for (final TreePath path : remotePaths) {
        w.addToEdits(path); // add and continue
        // To avoid holding the lock too long, we released it periodically
        if (Time.monotonicNow() - lastLock > lockReleasePeriod) {
          fsn.writeUnlock(opName);
          fsn.writeLock();
          lastLock = Time.monotonicNow();
        }
      }
    } finally {
      fsn.writeUnlock(opName);
    }
  }

  /**
   * Get the list of remote paths that belong to the sub-tree specified by
   * {@code remotePath}.
   * @param conf Configuration.
   * @param remotePath Remote path to mount.
   * @param rootOfMount Root of the mount point.
   * @return List of remote paths.
   */
  static List<TreePath> getRemotePaths(Configuration conf,
      String remotePath, String rootOfMount) throws IOException {
    final List<TreePath> remotePaths = new ArrayList<>();
    final Path[] localMounts = new Path[] {
        new Path(rootOfMount)
    };
    final TreeWalk[] remoteTreeWalks = new TreeWalk[] {
        new FSTreeWalk(remotePath, conf)
    };

    try (SimpleMountMetadataWriter w = new SimpleMountMetadataWriter()) {
      for (TreePath e : new FSMultiRootTreeWalk(localMounts,
          remoteTreeWalks)) {
        remotePaths.add(e);
        w.accept(e); // add and continue
      }
    }
    return remotePaths;
  }
}
