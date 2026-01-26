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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;

import java.util.Map;

/**
 * This interface is used for retrieving the load related statistics of
 * the cluster.
 */
@InterfaceAudience.Private
public interface FSClusterStats {

  /**
   * an indication of the total load of the cluster.
   *
   * @return a count of the total number of block transfers and block
   *         writes that are currently occuring on the cluster.
   */
  public int getTotalLoad();

  /**
   * Indicate whether or not the cluster is now avoiding
   * to use stale DataNodes for writing.
   *
   * @return True if the cluster is currently avoiding using stale DataNodes
   *         for writing targets, and false otherwise.
   */
  public boolean isAvoidingStaleDataNodesForWrite();

  /**
   * Indicates number of datanodes that are in service.
   * @return Number of datanodes that are both alive and not decommissioned.
   */
  public int getNumDatanodesInService();

  /**
   * An indication of the average load of non-decommission(ing|ed) nodes
   * eligible for block placement.
   *
   * @return average of the in service number of block transfers and block
   *         writes that are currently occurring on the cluster.
   */
  public double getInServiceXceiverAverage();

  /**
   * An indication of the average load of volumes at non-decommission(ing|ed)
   * nodes eligible for block placement.
   *
   * @return average of in service number of block transfers and block
   *         writes that are currently occurring on the volumes of the
   *         cluster.
   */
  double getInServiceXceiverAverageForVolume();

  /**
   * Indicates the storage statistics per storage type.
   * @return storage statistics per storage type.
   */
  Map<StorageType, StorageTypeStats> getStorageTypeStats();
}
