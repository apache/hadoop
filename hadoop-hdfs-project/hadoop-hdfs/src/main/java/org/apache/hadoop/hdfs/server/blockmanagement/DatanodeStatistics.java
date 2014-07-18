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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.protocol.ClientProtocol;

/** Datanode statistics */
public interface DatanodeStatistics {

  /** @return the total capacity */
  public long getCapacityTotal();

  /** @return the used capacity */
  public long getCapacityUsed();

  /** @return the percentage of the used capacity over the total capacity. */
  public float getCapacityUsedPercent();

  /** @return the remaining capacity */
  public long getCapacityRemaining();

  /** @return the percentage of the remaining capacity over the total capacity. */
  public float getCapacityRemainingPercent();

  /** @return the block pool used. */
  public long getBlockPoolUsed();

  /** @return the percentage of the block pool used space over the total capacity. */
  public float getPercentBlockPoolUsed();
  
  /** @return the total cache capacity of all DataNodes */
  public long getCacheCapacity();

  /** @return the total cache used by all DataNodes */
  public long getCacheUsed();

  /** @return the xceiver count */
  public int getXceiverCount();

  /** @return average xceiver count for non-decommission(ing|ed) nodes */
  public int getInServiceXceiverCount();
  
  /** @return number of non-decommission(ing|ed) nodes */
  public int getNumDatanodesInService();
  
  /**
   * @return the total used space by data nodes for non-DFS purposes
   * such as storing temporary files on the local file system
   */
  public long getCapacityUsedNonDFS();

  /** The same as {@link ClientProtocol#getStats()}.
   * The block related entries are set to -1.
   */
  public long[] getStats();

  /** @return the expired heartbeats */
  public int getExpiredHeartbeats();
}