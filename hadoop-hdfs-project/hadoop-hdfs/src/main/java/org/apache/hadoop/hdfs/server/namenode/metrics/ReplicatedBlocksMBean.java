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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This interface defines the methods to get status pertaining to blocks of type
 * {@link org.apache.hadoop.hdfs.protocol.BlockType#CONTIGUOUS} in FSNamesystem
 * of a NameNode. It is also used for publishing via JMX.
 * <p>
 * Aggregated status of all blocks is reported in
 * @see FSNamesystemMBean
 * Name Node runtime activity statistic info is reported in
 * @see org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics
 */
@InterfaceAudience.Private
public interface ReplicatedBlocksMBean {
  /**
   * Return low redundancy blocks count.
   */
  long getLowRedundancyReplicatedBlocks();

  /**
   * Return corrupt blocks count.
   */
  long getCorruptReplicatedBlocks();

  /**
   * Return missing blocks count.
   */
  long getMissingReplicatedBlocks();

  /**
   * Return count of missing blocks with replication factor one.
   */
  long getMissingReplicationOneBlocks();

  /**
   * Return total bytes of future blocks.
   */
  long getBytesInFutureReplicatedBlocks();

  /**
   * Return count of blocks that are pending deletion.
   */
  long getPendingDeletionReplicatedBlocks();

  /**
   * Return total number of replicated blocks.
   */
  long getTotalReplicatedBlocks();
}
