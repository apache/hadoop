/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.util.Time;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class extends the primary identifier of a Datanode with ephemeral
 * state, eg last reported time, usage information etc.
 */
public class DatanodeInfo extends DatanodeDetails {

  private final ReadWriteLock lock;

  private volatile long lastHeartbeatTime;
  private long lastStatsUpdatedTime;

  private List<StorageReportProto> storageReports;

  private NodeStatus nodeStatus;

  /**
   * Constructs DatanodeInfo from DatanodeDetails.
   *
   * @param datanodeDetails Details about the datanode
   */
  public DatanodeInfo(DatanodeDetails datanodeDetails, NodeStatus nodeStatus) {
    super(datanodeDetails);
    this.lock = new ReentrantReadWriteLock();
    this.lastHeartbeatTime = Time.monotonicNow();
    this.storageReports = Collections.emptyList();
    this.nodeStatus = nodeStatus;
  }

  /**
   * Updates the last heartbeat time with current time.
   */
  public void updateLastHeartbeatTime() {
    updateLastHeartbeatTime(Time.monotonicNow());
  }

  /**
   * Sets the last heartbeat time to a given value. Intended to be used
   * only for tests.
   *
   * @param milliSecondsSinceEpoch - ms since Epoch to set as the heartbeat time
   */
  @VisibleForTesting
  public void updateLastHeartbeatTime(long milliSecondsSinceEpoch) {
    try {
      lock.writeLock().lock();
      lastHeartbeatTime = milliSecondsSinceEpoch;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns the last heartbeat time.
   *
   * @return last heartbeat time.
   */
  public long getLastHeartbeatTime() {
    try {
      lock.readLock().lock();
      return lastHeartbeatTime;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Updates the datanode storage reports.
   *
   * @param reports list of storage report
   */
  public void updateStorageReports(List<StorageReportProto> reports) {
    try {
      lock.writeLock().lock();
      lastStatsUpdatedTime = Time.monotonicNow();
      storageReports = reports;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns the storage reports associated with this datanode.
   *
   * @return list of storage report
   */
  public List<StorageReportProto> getStorageReports() {
    try {
      lock.readLock().lock();
      return storageReports;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Return the current NodeStatus for the datanode.
   *
   * @return NodeStatus - the current nodeStatus
   */
  public NodeStatus getNodeStatus() {
    try {
      lock.readLock().lock();
      return nodeStatus;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Update the NodeStatus for this datanode. When using this method
   * be ware of the potential for lost updates if two threads read the
   * current status, update one field and then write it back without
   * locking enforced outside of this class.
   *
   * @param newNodeStatus - the new NodeStatus object
   */
  public void setNodeStatus(NodeStatus newNodeStatus) {
    try {
      lock.writeLock().lock();
      this.nodeStatus = newNodeStatus;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns the last updated time of datanode info.
   * @return the last updated time of datanode info.
   */
  public long getLastStatsUpdatedTime() {
    return lastStatsUpdatedTime;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

}
