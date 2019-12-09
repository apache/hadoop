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

import java.beans.ConstructorProperties;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;

/**
 * Statistics per StorageType.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class StorageTypeStats {
  private long capacityTotal = 0L;
  private long capacityUsed = 0L;
  private long capacityNonDfsUsed = 0L;
  private long capacityRemaining = 0L;
  private long blockPoolUsed = 0L;
  private int nodesInService = 0;
  private StorageType storageType;

  @ConstructorProperties({"capacityTotal", "capacityUsed", "capacityNonDfsUsed",
      "capacityRemaining", "blockPoolUsed", "nodesInService"})
  public StorageTypeStats(
      long capacityTotal, long capacityUsed, long capacityNonDfsUsedUsed,
      long capacityRemaining, long blockPoolUsed, int nodesInService) {
    this.capacityTotal = capacityTotal;
    this.capacityUsed = capacityUsed;
    this.capacityNonDfsUsed = capacityNonDfsUsedUsed;
    this.capacityRemaining = capacityRemaining;
    this.blockPoolUsed = blockPoolUsed;
    this.nodesInService = nodesInService;
  }

  public long getCapacityTotal() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityTotal/nodesInService;
    }
    return capacityTotal;
  }

  public long getCapacityUsed() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityUsed/nodesInService;
    }
    return capacityUsed;
  }

  public long getCapacityNonDfsUsed() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityNonDfsUsed/nodesInService;
    }
    return capacityNonDfsUsed;
  }

  public long getCapacityRemaining() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityRemaining/nodesInService;
    }
    return capacityRemaining;
  }

  public long getBlockPoolUsed() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return blockPoolUsed/nodesInService;
    }
    return blockPoolUsed;
  }

  public int getNodesInService() {
    return nodesInService;
  }

  StorageTypeStats(StorageType storageType) {
    this.storageType = storageType;
  }

  StorageTypeStats(StorageTypeStats other) {
    capacityTotal = other.capacityTotal;
    capacityUsed = other.capacityUsed;
    capacityNonDfsUsed = other.capacityNonDfsUsed;
    capacityRemaining = other.capacityRemaining;
    blockPoolUsed = other.blockPoolUsed;
    nodesInService = other.nodesInService;
  }

  void addStorage(final DatanodeStorageInfo info,
      final DatanodeDescriptor node) {
    assert storageType == info.getStorageType();
    capacityUsed += info.getDfsUsed();
    capacityNonDfsUsed += info.getNonDfsUsed();
    blockPoolUsed += info.getBlockPoolUsed();
    if (node.isInService()) {
      capacityTotal += info.getCapacity();
      capacityRemaining += info.getRemaining();
    } else {
      capacityTotal += info.getDfsUsed();
    }
  }

  void addNode(final DatanodeDescriptor node) {
    if (node.isInService()) {
      nodesInService++;
    }
  }

  void subtractStorage(final DatanodeStorageInfo info,
      final DatanodeDescriptor node) {
    assert storageType == info.getStorageType();
    capacityUsed -= info.getDfsUsed();
    capacityNonDfsUsed -= info.getNonDfsUsed();
    blockPoolUsed -= info.getBlockPoolUsed();
    if (node.isInService()) {
      capacityTotal -= info.getCapacity();
      capacityRemaining -= info.getRemaining();
    } else {
      capacityTotal -= info.getDfsUsed();
    }
  }

  void subtractNode(final DatanodeDescriptor node) {
    if (node.isInService()) {
      nodesInService--;
    }
  }
}
