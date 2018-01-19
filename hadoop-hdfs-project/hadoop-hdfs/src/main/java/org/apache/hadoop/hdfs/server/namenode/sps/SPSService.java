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
   * @param fileIDCollector
   *          - a helper service for scanning the files under a given directory
   *          id
   * @param handler
   *          - a helper service for moving the blocks
   */
  void init(Context ctxt, FileIdCollector fileIDCollector,
      BlockMoveTaskHandler handler);

  /**
   * Starts the SPS service. Make sure to initialize the helper services before
   * invoking this method.
   *
   * @param reconfigStart
   *          - to indicate whether the SPS startup requested from
   *          reconfiguration service
   */
  void start(boolean reconfigStart);

  /**
   * Stops the SPS service gracefully. Timed wait to stop storage policy
   * satisfier daemon threads.
   */
  void stopGracefully();

  /**
   * Disable the SPS service.
   *
   * @param forceStop
   */
  void disable(boolean forceStop);

  /**
   * Check whether StoragePolicySatisfier is running.
   *
   * @return true if running
   */
  boolean isRunning();

  /**
   * Adds the Item information(file id etc) to processing queue.
   *
   * @param itemInfo
   */
  void addFileIdToProcess(ItemInfo itemInfo);

  /**
   * Adds all the Item information(file id etc) to processing queue.
   *
   * @param startId
   *          - directory/file id, on which SPS was called.
   * @param itemInfoList
   *          - list of item infos
   * @param scanCompleted
   *          - whether the scanning of directory fully done with itemInfoList
   */
  void addAllFileIdsToProcess(long startId, List<ItemInfo> itemInfoList,
      boolean scanCompleted);

  /**
   * @return current processing queue size.
   */
  int processingQueueSize();

  /**
   * @return the configuration.
   */
  Configuration getConf();
}
