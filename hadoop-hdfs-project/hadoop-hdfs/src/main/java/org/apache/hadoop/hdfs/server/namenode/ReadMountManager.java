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
import org.apache.hadoop.fs.MountMode;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.TreePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_MOUNT_INODES_MAX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_MOUNT_INODES_MAX_DEFAULT;

/**
 * Plays a role to maintain readOnly mount.
 */
public final class ReadMountManager implements MountManagerSpi {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReadMountManager.class);
  public static final MountMode MODE = MountMode.READONLY;
  private FSNamesystem fsNamesystem;

  private static ReadMountManager manager = null;

  private ReadMountManager(Configuration conf, FSNamesystem fsNamesystem) {
    this.fsNamesystem = fsNamesystem;
  }

  public static ReadMountManager getInstance(Configuration conf,
      FSNamesystem fsNamesystem) {
    if (manager == null) {
      manager = new ReadMountManager(conf, fsNamesystem);
      return manager;
    }
    return manager;
  }

  @Override
  public boolean isSupervisedMode(MountMode mountMode) {
    return MODE == mountMode;
  }

  /**
   * Prepare adding new mount. The remote paths will be obtained and
   * the corresponding iNodes will be persisted in namespace.
   */
  @Override
  public void prepareMount(String tempMountPath, String remotePath,
      Configuration mergedConf) throws IOException {
    // Verify remote connection.
    verifyConnection(remotePath, mergedConf);
    // Get remote paths without lock
    final List<TreePath> remotePaths;
    remotePaths =
        FSMountAttrOp.getRemotePaths(mergedConf, remotePath, tempMountPath);
    // ensure that number of Inodes is fewer than max allowed!
    checkMaxInodesAllowed(mergedConf, remotePaths.size());
    LOG.info("Adding " + remotePaths.size() + " paths from " + remotePath +
        " into HDFS path");
    FSMountAttrOp.addRemotePaths(fsNamesystem, mergedConf,
        MountManager.MOUNT_OP, remotePaths);
  }

  /**
   * Checks that the number of inodes for the mount is fewer than
   * the max allowed.
   *
   * @param conf Configuration
   * @param numRemotePaths remote paths to mount
   * @throws IOException thrown if the number of inodes exceed the max allowed.
   */
  private void checkMaxInodesAllowed(Configuration conf,
      int numRemotePaths) throws IOException {

    int maxInodesAllowed = conf.getInt(DFS_PROVIDED_READ_MOUNT_INODES_MAX,
        DFS_PROVIDED_READ_MOUNT_INODES_MAX_DEFAULT);
    // -1 to account for "/" being counted as one of the remote paths
    int inodesInMount = numRemotePaths - 1;
    if (maxInodesAllowed >= 0 && inodesInMount > maxInodesAllowed) {
      throw new IOException("Mount failed! Number of inodes in remote path (" +
          inodesInMount + ") exceed the maximum allowed of " +
          maxInodesAllowed);
    }
  }

  @Override
  public void finishMount(ProvidedVolumeInfo volInfo, boolean isMounted)
      throws IOException {
  }

  @Override
  public void removeMount(ProvidedVolumeInfo volInfo) throws IOException {
  }

  @Override
  public void startService() throws IOException {
  }

  @Override
  public void stopService()  {
  }

  /**
   * Currently, there is no metric for a specific mounted storage in readOnly
   * mount mode.
   */
  @Override
  public String getMetrics(ProvidedVolumeInfo volumeInfo) {
    return "";
  }

}
