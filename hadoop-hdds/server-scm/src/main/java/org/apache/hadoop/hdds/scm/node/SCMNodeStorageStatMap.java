/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.node;


import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.DUPLICATE_DATANODE;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.NO_SUCH_DATANODE;

/**
 * This data structure maintains the disk space capacity, disk usage and free
 * space availability per Datanode.
 * This information is built from the DN node reports.
 */
public class SCMNodeStorageStatMap implements SCMNodeStorageStatMXBean {
  static final Logger LOG =
      LoggerFactory.getLogger(SCMNodeStorageStatMap.class);

  private final double warningUtilizationThreshold;
  private final double criticalUtilizationThreshold;

  private final Map<UUID, SCMNodeStat> scmNodeStorageStatMap;
  // NodeStorageInfo MXBean
  private ObjectName scmNodeStorageInfoBean;
  // Aggregated node stats
  private SCMNodeStat clusterStat;
  /**
   * constructs the scmNodeStorageStatMap object
   */
  public SCMNodeStorageStatMap(OzoneConfiguration conf) {
    scmNodeStorageStatMap = new ConcurrentHashMap<>();
    warningUtilizationThreshold = conf.getDouble(
        OzoneConfigKeys.
            HDDS_DATANODE_STORAGE_UTILIZATION_WARNING_THRESHOLD,
        OzoneConfigKeys.
            HDDS_DATANODE_STORAGE_UTILIZATION_WARNING_THRESHOLD_DEFAULT);
    criticalUtilizationThreshold = conf.getDouble(
        OzoneConfigKeys.
            HDDS_DATANODE_STORAGE_UTILIZATION_CRITICAL_THRESHOLD,
        OzoneConfigKeys.
            HDDS_DATANODE_STORAGE_UTILIZATION_CRITICAL_THRESHOLD_DEFAULT);
    clusterStat = new SCMNodeStat();
  }

  public enum UtilizationThreshold {
    NORMAL, WARN, CRITICAL;
  }

  /**
   * Returns true if this a datanode that is already tracked by
   * scmNodeStorageStatMap.
   *
   * @param datanodeID - UUID of the Datanode.
   * @return True if this is tracked, false if this map does not know about it.
   */
  public boolean isKnownDatanode(UUID datanodeID) {
    Preconditions.checkNotNull(datanodeID);
    return scmNodeStorageStatMap.containsKey(datanodeID);
  }

  public List<UUID> getDatanodeList(
      UtilizationThreshold threshold) {
    return scmNodeStorageStatMap.entrySet().stream()
        .filter(entry -> (isThresholdReached(threshold, entry.getValue())))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }



  /**
   * Insert a new datanode into Node2Container Map.
   *
   * @param datanodeID -- Datanode UUID
   * @param stat - scmNode stat for the Datanode.
   */
  public void insertNewDatanode(UUID datanodeID, SCMNodeStat stat)
      throws SCMException {
    Preconditions.checkNotNull(stat);
    Preconditions.checkNotNull(datanodeID);
    synchronized (scmNodeStorageStatMap) {
      if (isKnownDatanode(datanodeID)) {
        throw new SCMException("Node already exists in the map",
            DUPLICATE_DATANODE);
      }
      scmNodeStorageStatMap.put(datanodeID, stat);
      clusterStat.add(stat);
    }
  }

  //TODO: This should be called once SCMNodeManager gets Started.
  private void registerMXBean() {
    this.scmNodeStorageInfoBean = MBeans.register("StorageContainerManager",
        "scmNodeStorageInfo", this);
  }

  //TODO: Unregister call should happen as a part of SCMNodeManager shutdown.
  private void unregisterMXBean() {
    if(this.scmNodeStorageInfoBean != null) {
      MBeans.unregister(this.scmNodeStorageInfoBean);
      this.scmNodeStorageInfoBean = null;
    }
  }
  /**
   * Updates the Container list of an existing DN.
   *
   * @param datanodeID - UUID of DN.
   * @param stat - scmNode stat for the Datanode.
   * @throws SCMException - if we don't know about this datanode, for new DN
   *                      use insertNewDatanode.
   */
  public void updateDatanodeMap(UUID datanodeID, SCMNodeStat stat)
      throws SCMException {
    Preconditions.checkNotNull(datanodeID);
    Preconditions.checkNotNull(stat);
    synchronized (scmNodeStorageStatMap) {
      if (!scmNodeStorageStatMap.containsKey(datanodeID)) {
        throw new SCMException("No such datanode", NO_SUCH_DATANODE);
      }
      SCMNodeStat removed = scmNodeStorageStatMap.get(datanodeID);
      clusterStat.subtract(removed);
      scmNodeStorageStatMap.put(datanodeID, stat);
      clusterStat.add(stat);
    }
  }

  public NodeReportStatus processNodeReport(UUID datanodeID,
      StorageContainerDatanodeProtocolProtos.SCMNodeReport nodeReport)
      throws SCMException {
    Preconditions.checkNotNull(datanodeID);
    Preconditions.checkNotNull(nodeReport);
    long totalCapacity = 0;
    long totalRemaining = 0;
    long totalScmUsed = 0;
    List<StorageContainerDatanodeProtocolProtos.SCMStorageReport>
        storageReports = nodeReport.getStorageReportList();
    for (StorageContainerDatanodeProtocolProtos.SCMStorageReport report : storageReports) {
      totalCapacity += report.getCapacity();
      totalRemaining += report.getRemaining();
      totalScmUsed += report.getScmUsed();
    }
    SCMNodeStat stat = scmNodeStorageStatMap.get(datanodeID);
    if (stat == null) {
      stat = new SCMNodeStat();
      stat.set(totalCapacity, totalScmUsed, totalRemaining);
      insertNewDatanode(datanodeID, stat);
    } else {
      stat.set(totalCapacity, totalScmUsed, totalRemaining);
      updateDatanodeMap(datanodeID, stat);
    }
    if (isThresholdReached(UtilizationThreshold.CRITICAL, stat)) {
      LOG.warn("Datanode {} is out of storage space. Capacity: {}, Used: {}",
          datanodeID, stat.getCapacity().get(), stat.getScmUsed().get());
      return NodeReportStatus.DATANODE_OUT_OF_SPACE;
    } else {
      if (isThresholdReached(UtilizationThreshold.WARN, stat)) {
       LOG.warn("Datanode {} is low on storage space. Capacity: {}, Used: {}",
           datanodeID, stat.getCapacity().get(), stat.getScmUsed().get());
      }
      return NodeReportStatus.ALL_IS_WELL;
    }
  }

  private boolean isThresholdReached(UtilizationThreshold threshold,
      SCMNodeStat stat) {
    switch (threshold) {
    case NORMAL:
      return stat.getScmUsedratio() < warningUtilizationThreshold;
    case WARN:
      return stat.getScmUsedratio() >= warningUtilizationThreshold &&
          stat.getScmUsedratio() < criticalUtilizationThreshold;
    case CRITICAL:
      return stat.getScmUsedratio() >= criticalUtilizationThreshold;
    default:
      throw new RuntimeException("Unknown UtilizationThreshold value");
    }
  }

  @Override
  public long getCapacity(UUID dnId) {
    return scmNodeStorageStatMap.get(dnId).getCapacity().get();
  }

  @Override
  public long getRemainingSpace(UUID dnId) {
    return scmNodeStorageStatMap.get(dnId).getRemaining().get();
  }

  @Override
  public long getUsedSpace(UUID dnId) {
    return scmNodeStorageStatMap.get(dnId).getScmUsed().get();
  }

  @Override
  public long getTotalCapacity() {
    return clusterStat.getCapacity().get();
  }

  @Override
  public long getTotalSpaceUsed() {
    return clusterStat.getScmUsed().get();
  }

  @Override
  public long getTotalFreeSpace() {
    return clusterStat.getRemaining().get();
  }

  /**
   * removes the dataNode from scmNodeStorageStatMap
   * @param datanodeID
   * @throws SCMException in case the dataNode is not found in the map.
   */
  public void removeDatanode(UUID datanodeID) throws SCMException {
    Preconditions.checkNotNull(datanodeID);
    synchronized (scmNodeStorageStatMap) {
      if (!scmNodeStorageStatMap.containsKey(datanodeID)) {
        throw new SCMException("No such datanode", NO_SUCH_DATANODE);
      }
      SCMNodeStat stat = scmNodeStorageStatMap.remove(datanodeID);
      clusterStat.subtract(stat);
    }
  }

  /**
   * Gets the SCMNodeStat for the datanode
   * @param  datanodeID
   * @return SCMNodeStat
   */

  SCMNodeStat getNodeStat(UUID datanodeID) {
    return scmNodeStorageStatMap.get(datanodeID);
  }

  /**
   * Results possible from processing a Node report by
   * Node2ContainerMapper.
   */
  public enum NodeReportStatus {
    ALL_IS_WELL,
    DATANODE_OUT_OF_SPACE
  }

}
