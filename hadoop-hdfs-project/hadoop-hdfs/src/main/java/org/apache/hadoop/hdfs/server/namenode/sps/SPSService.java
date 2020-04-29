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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;

/**
 * An interface for SPSService, which exposes life cycle and processing APIs.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface SPSService {

  /**
   * Initializes the helper services.
   *
   * @param ctxt
   *          - context is an helper service to provide communication channel
   *          between NN and SPS
   */
  void init(Context ctxt);

  /**
   * Starts the SPS service. Make sure to initialize the helper services before
   * invoking this method.
   *
   * @param spsMode sps service mode
   */
  void start(StoragePolicySatisfierMode spsMode);

  /**
   * Stops the SPS service gracefully. Timed wait to stop storage policy
   * satisfier daemon threads.
   */
  void stopGracefully();

  /**
   * Stops the SPS service.
   *
   * @param forceStop
   *          true represents to clear all the sps path's hint, false otherwise.
   */
  void stop(boolean forceStop);

  /**
   * Check whether StoragePolicySatisfier is running.
   *
   * @return true if running
   */
  boolean isRunning();

  /**
   * Adds the Item information(file etc) to processing queue.
   *
   * @param itemInfo
   *          file info object for which need to satisfy the policy
   */
  void addFileToProcess(ItemInfo itemInfo, boolean scanCompleted);

  /**
   * Adds all the Item information(file etc) to processing queue.
   *
   * @param startPathId
   *          - directoryId/fileId, on which SPS was called.
   * @param itemInfoList
   *          - list of item infos
   * @param scanCompleted
   *          - whether the scanning of directory fully done with itemInfoList
   */
  void addAllFilesToProcess(long startPathId, List<ItemInfo> itemInfoList,
      boolean scanCompleted);

  /**
   * @return current processing queue size.
   */
  int processingQueueSize();

  /**
   * @return the configuration.
   */
  Configuration getConf();

  /**
   * Marks the scanning of directory if finished.
   *
   * @param spsPath
   *          - satisfier path id
   */
  void markScanCompletedForPath(long spsPath);

  /**
   * Given node is reporting that it received a certain movement attempt
   * finished block.
   *
   * @param dnInfo
   *          - reported datanode
   * @param storageType
   *          - storage type
   * @param block
   *          - block that is attempted to move
   */
  void notifyStorageMovementAttemptFinishedBlk(DatanodeInfo dnInfo,
      StorageType storageType, Block block);
}
