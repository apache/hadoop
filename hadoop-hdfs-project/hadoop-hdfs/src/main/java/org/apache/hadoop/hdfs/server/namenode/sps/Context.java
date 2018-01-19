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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.AccessControlException;

/**
 * An interface for the communication between NameNode and SPS module.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface Context {

  /**
   * Returns true if the SPS is running, false otherwise.
   */
  boolean isRunning();

  /**
   * Returns true if the Namenode in safe mode, false otherwise.
   */
  boolean isInSafeMode();

  /**
   * Returns true if Mover tool is already running, false otherwise.
   */
  boolean isMoverRunning();

  /**
   * Gets the Inode ID number for the given path.
   *
   * @param path
   *          - file/dir path
   * @return Inode id number
   */
  long getFileID(String path) throws UnresolvedLinkException,
      AccessControlException, ParentNotDirectoryException;

  /**
   * Gets the network topology.
   *
   * @return network topology
   */
  NetworkTopology getNetworkTopology();

  /**
   * Returns true if the give Inode exists in the Namespace.
   *
   * @param inodeId
   *          - Inode ID
   * @return true if Inode exists, false otherwise.
   */
  boolean isFileExist(long inodeId);

  /**
   * Gets the storage policy details for the given policy ID.
   *
   * @param policyId
   *          - Storage policy ID
   * @return the detailed policy object
   */
  BlockStoragePolicy getStoragePolicy(byte policyId);

  /**
   * Drop the SPS work in case if any previous work queued up.
   */
  void addDropPreviousSPSWorkAtDNs();

  /**
   * Remove the hint which was added to track SPS call.
   *
   * @param inodeId
   *          - Inode ID
   * @throws IOException
   */
  void removeSPSHint(long inodeId) throws IOException;

  /**
   * Gets the number of live datanodes in the cluster.
   *
   * @return number of live datanodes
   */
  int getNumLiveDataNodes();

  /**
   * Get the file info for a specific file.
   *
   * @param inodeID
   *          inode identifier
   * @return file status metadata information
   */
  HdfsFileStatus getFileInfo(long inodeID) throws IOException;

  /**
   * Returns all the live datanodes and its storage details.
   *
   * @throws IOException
   */
  DatanodeStorageReport[] getLiveDatanodeStorageReport()
      throws IOException;

  /**
   * Returns true if the given inode file has low redundancy blocks.
   *
   * @param inodeID
   *          inode identifier
   * @return true if block collection has low redundancy blocks
   */
  boolean hasLowRedundancyBlocks(long inodeID);

  /**
   * Checks whether the given datanode has sufficient space to occupy the given
   * blockSize data.
   *
   * @param dn
   *          datanode info
   * @param type
   *          storage type
   * @param blockSize
   *          blockSize to be scheduled
   * @return true if the given datanode has sufficient space to occupy blockSize
   *         data, false otherwise.
   */
  boolean verifyTargetDatanodeHasSpaceForScheduling(DatanodeInfo dn,
      StorageType type, long blockSize);

  /**
   * @return next SPS path id to process.
   */
  Long getNextSPSPathId();

  /**
   * Removes the SPS path id.
   */
  void removeSPSPathId(long pathId);

  /**
   * Removes all SPS path ids.
   */
  void removeAllSPSPathIds();

}
