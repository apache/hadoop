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
package org.apache.hadoop.hdfs.server.namenode.sps;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.DatanodeMap;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the Namenode implementation for analyzing the file blocks which
 * are expecting to change its storages and assigning the block storage
 * movements to satisfy the storage policy.
 */
@InterfaceAudience.Private
public class IntraSPSNameNodeContext implements Context {
  private static final Logger LOG = LoggerFactory
      .getLogger(IntraSPSNameNodeContext.class);

  private final Namesystem namesystem;
  private final BlockManager blockManager;

  private SPSService service;
  private final FileCollector fileCollector;
  private final BlockMoveTaskHandler blockMoveTaskHandler;

  public IntraSPSNameNodeContext(Namesystem namesystem,
      BlockManager blockManager, SPSService service) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.service = service;
    fileCollector = new IntraSPSNameNodeFileIdCollector(
        namesystem.getFSDirectory(), service);
    blockMoveTaskHandler = new IntraSPSNameNodeBlockMoveTaskHandler(
        blockManager, namesystem);
  }

  @Override
  public int getNumLiveDataNodes() {
    return blockManager.getDatanodeManager().getNumLiveDataNodes();
  }

  /**
   * @return object containing information regarding the file.
   */
  @Override
  public HdfsFileStatus getFileInfo(long inodeID) throws IOException {
    Path filePath = DFSUtilClient.makePathFromFileId(inodeID);
    return namesystem.getFileInfo(filePath.toString(), true, true);
  }

  @Override
  public DatanodeStorageReport[] getLiveDatanodeStorageReport()
      throws IOException {
    namesystem.readLock();
    try {
      return blockManager.getDatanodeManager()
          .getDatanodeStorageReport(DatanodeReportType.LIVE);
    } finally {
      namesystem.readUnlock();
    }
  }

  @Override
  public boolean isFileExist(long inodeId) {
    return namesystem.getFSDirectory().getInode(inodeId) != null;
  }

  @Override
  public void removeSPSHint(long inodeId) throws IOException {
    this.namesystem.removeXattr(inodeId, XATTR_SATISFY_STORAGE_POLICY);
  }

  @Override
  public boolean isRunning() {
    return namesystem.isRunning() && service.isRunning();
  }

  @Override
  public boolean isInSafeMode() {
    return namesystem.isInSafeMode();
  }

  @Override
  public boolean isMoverRunning() {
    String moverId = HdfsServerConstants.MOVER_ID_PATH.toString();
    return namesystem.isFileOpenedForWrite(moverId);
  }

  @Override
  public void addDropPreviousSPSWorkAtDNs() {
    namesystem.readLock();
    try {
      blockManager.getDatanodeManager().addDropSPSWorkCommandsToAllDNs();
    } finally {
      namesystem.readUnlock();
    }
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(byte policyID) {
    return blockManager.getStoragePolicy(policyID);
  }

  @Override
  public NetworkTopology getNetworkTopology(DatanodeMap datanodeMap) {
    return blockManager.getDatanodeManager().getNetworkTopology();
  }

  @Override
  public long getFileID(String path) throws UnresolvedLinkException,
      AccessControlException, ParentNotDirectoryException {
    namesystem.readLock();
    try {
      INode inode = namesystem.getFSDirectory().getINode(path);
      return inode == null ? -1 : inode.getId();
    } finally {
      namesystem.readUnlock();
    }
  }

  @Override
  public Long getNextSPSPath() {
    return blockManager.getSPSManager().getNextPathId();
  }

  @Override
  public void removeSPSPathId(long trackId) {
    blockManager.getSPSManager().removePathId(trackId);
  }

  @Override
  public void removeAllSPSPathIds() {
    blockManager.getSPSManager().removeAllPathIds();
  }

  @Override
  public void scanAndCollectFiles(long filePath)
      throws IOException, InterruptedException {
    fileCollector.scanAndCollectFiles(filePath);
  }

  @Override
  public void submitMoveTask(BlockMovingInfo blkMovingInfo) throws IOException {
    blockMoveTaskHandler.submitMoveTask(blkMovingInfo);
  }

  @Override
  public void notifyMovementTriedBlocks(Block[] moveAttemptFinishedBlks) {
    LOG.info("Movement attempted blocks: {}",
        Arrays.asList(moveAttemptFinishedBlks));
  }
}
