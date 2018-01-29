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

package org.apache.hadoop.hdfs.server.sps;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.sps.Context;
import org.apache.hadoop.hdfs.server.namenode.sps.SPSService;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class used to connect to Namenode and gets the required information to
 * SPS from Namenode state.
 */
@InterfaceAudience.Private
public class ExternalSPSContext implements Context {
  public static final Logger LOG =
      LoggerFactory.getLogger(ExternalSPSContext.class);
  private SPSService service;
  private NameNodeConnector nnc = null;
  private BlockStoragePolicySuite createDefaultSuite =
      BlockStoragePolicySuite.createDefaultSuite();

  public ExternalSPSContext(SPSService service, NameNodeConnector nnc) {
    this.service = service;
    this.nnc = nnc;
  }

  @Override
  public boolean isRunning() {
    return service.isRunning();
  }

  @Override
  public boolean isInSafeMode() {
    try {
      return nnc != null ? nnc.getDistributedFileSystem().isInSafeMode()
          : false;
    } catch (IOException e) {
      LOG.warn("Exception while creating Namenode Connector..", e);
      return false;
    }
  }

  @Override
  public boolean isMoverRunning() {
    try {
      FSDataOutputStream out = nnc.getDistributedFileSystem()
          .append(HdfsServerConstants.MOVER_ID_PATH);
      out.close();
      return false;
    } catch (IOException ioe) {
      LOG.warn("Exception while checking mover is running..", ioe);
      return true;
    }

  }

  @Override
  public long getFileID(String path) throws UnresolvedLinkException,
      AccessControlException, ParentNotDirectoryException {
    HdfsFileStatus fs = null;
    try {
      fs = (HdfsFileStatus) nnc.getDistributedFileSystem().getFileStatus(
          new Path(path));
      LOG.info("Fetched the fileID:{} for the path:{}", fs.getFileId(), path);
    } catch (IllegalArgumentException | IOException e) {
      LOG.warn("Exception while getting file is for the given path:{}.", path,
          e);
    }
    return fs != null ? fs.getFileId() : 0;
  }

  @Override
  public NetworkTopology getNetworkTopology() {
    return NetworkTopology.getInstance(service.getConf());
  }

  @Override
  public boolean isFileExist(long inodeId) {
    String filePath = null;
    try {
      filePath = getFilePath(inodeId);
      return nnc.getDistributedFileSystem().exists(new Path(filePath));
    } catch (IllegalArgumentException | IOException e) {
      LOG.warn("Exception while getting file is for the given path:{} "
          + "and fileId:{}", filePath, inodeId, e);
    }
    return false;
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(byte policyId) {
    return createDefaultSuite.getPolicy(policyId);
  }

  @Override
  public void addDropPreviousSPSWorkAtDNs() {
    // Nothing todo
  }

  @Override
  public void removeSPSHint(long inodeId) throws IOException {
    nnc.getDistributedFileSystem().removeXAttr(new Path(getFilePath(inodeId)),
        HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY);
  }

  @Override
  public int getNumLiveDataNodes() {
    try {
      return nnc.getDistributedFileSystem()
          .getDataNodeStats(DatanodeReportType.LIVE).length;
    } catch (IOException e) {
      LOG.warn("Exception while getting number of live datanodes.", e);
    }
    return 0;
  }

  @Override
  public HdfsFileStatus getFileInfo(long inodeID) throws IOException {
    return nnc.getDistributedFileSystem().getClient()
        .getLocatedFileInfo(getFilePath(inodeID), false);
  }

  @Override
  public DatanodeStorageReport[] getLiveDatanodeStorageReport()
      throws IOException {
    return nnc.getLiveDatanodeStorageReport();
  }

  @Override
  public boolean hasLowRedundancyBlocks(long inodeID) {
    try {
      return nnc.getNNProtocolConnection().hasLowRedundancyBlocks(inodeID);
    } catch (IOException e) {
      LOG.warn("Failed to check whether fileid:{} has low redundancy blocks.",
          inodeID, e);
      return false;
    }
  }

  @Override
  public boolean checkDNSpaceForScheduling(DatanodeInfo dn, StorageType type,
      long estimatedSize) {
    // TODO: Instead of calling namenode for checking the available space, it
    // can be optimized by maintaining local cache of datanode storage report
    // and do the computations. This local cache can be refreshed per file or
    // periodic fashion.
    try {
      return nnc.getNNProtocolConnection().checkDNSpaceForScheduling(dn, type,
          estimatedSize);
    } catch (IOException e) {
      LOG.warn("Verify the given datanode:{} is good and has "
          + "estimated space in it.", dn, e);
      return false;
    }
  }

  @Override
  public Long getNextSPSPathId() {
    try {
      return nnc.getNNProtocolConnection().getNextSPSPathId();
    } catch (IOException e) {
      LOG.warn("Exception while getting next sps path id from Namenode.", e);
      return null;
    }
  }

  @Override
  public void removeSPSPathId(long pathId) {
    // We need not specifically implement for external.
  }

  @Override
  public void removeAllSPSPathIds() {
    // We need not specifically implement for external.
  }

  @Override
  public String getFilePath(Long inodeId) {
    try {
      return nnc.getNNProtocolConnection().getFilePath(inodeId);
    } catch (IOException e) {
      LOG.warn("Exception while getting file path id:{} from Namenode.",
          inodeId, e);
      return null;
    }
  }
}
