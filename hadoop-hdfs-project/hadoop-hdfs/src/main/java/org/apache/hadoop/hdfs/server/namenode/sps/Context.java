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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.DatanodeMap;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.net.NetworkTopology;

/**
 * An interface for the communication between SPS and Namenode module.
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
   * Gets the network topology.
   *
   * @param datanodeMap
   *          target datanodes
   *
   * @return network topology
   */
  NetworkTopology getNetworkTopology(DatanodeMap datanodeMap);

  /**
   * Returns true if the give file exists in the Namespace.
   *
   * @param filePath
   *          - file info
   * @return true if the given file exists, false otherwise.
   */
  boolean isFileExist(long filePath);

  /**
   * Gets the storage policy details for the given policy ID.
   *
   * @param policyId
   *          - Storage policy ID
   * @return the detailed policy object
   */
  BlockStoragePolicy getStoragePolicy(byte policyId);

  /**
   * Remove the hint which was added to track SPS call.
   *
   * @param spsPath
   *          - user invoked satisfier path
   * @throws IOException
   */
  void removeSPSHint(long spsPath) throws IOException;

  /**
   * Gets the number of live datanodes in the cluster.
   *
   * @return number of live datanodes
   */
  int getNumLiveDataNodes();

  /**
   * Get the file info for a specific file.
   *
   * @param file
   *          file path
   * @return file status metadata information
   */
  HdfsFileStatus getFileInfo(long file) throws IOException;

  /**
   * Returns all the live datanodes and its storage details.
   *
   * @throws IOException
   */
  DatanodeStorageReport[] getLiveDatanodeStorageReport()
      throws IOException;

  /**
   * @return next SPS path info to process.
   */
  Long getNextSPSPath();

  /**
   * Do scan and collects the files under that directory and adds to the given
   * BlockStorageMovementNeeded.
   *
   * @param filePath
   *          file path
   */
  void scanAndCollectFiles(long filePath)
      throws IOException, InterruptedException;

  /**
   * Handles the block move tasks. BlockMovingInfo must contain the required
   * info to move the block, that source location, destination location and
   * storage types.
   */
  void submitMoveTask(BlockMovingInfo blkMovingInfo) throws IOException;

  /**
   * This can be used to notify to the SPS about block movement attempt
   * finished. Then SPS will re-check whether it needs retry or not.
   *
   * @param moveAttemptFinishedBlks
   *          list of movement attempt finished blocks
   */
  void notifyMovementTriedBlocks(Block[] moveAttemptFinishedBlks);
}
