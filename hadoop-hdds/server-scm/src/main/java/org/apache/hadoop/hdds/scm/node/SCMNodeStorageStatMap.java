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
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private final Map<UUID, Set<StorageLocationReport>> scmNodeStorageReportMap;
  // NodeStorageInfo MXBean
  private ObjectName scmNodeStorageInfoBean;
  /**
   * constructs the scmNodeStorageReportMap object.
   */
  public SCMNodeStorageStatMap(OzoneConfiguration conf) {
    // scmNodeStorageReportMap = new ConcurrentHashMap<>();
    scmNodeStorageReportMap = new ConcurrentHashMap<>();
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
  }

  /**
   * Enum that Describes what we should do at various thresholds.
   */
  public enum UtilizationThreshold {
    NORMAL, WARN, CRITICAL;
  }

  /**
   * Returns true if this a datanode that is already tracked by
   * scmNodeStorageReportMap.
   *
   * @param datanodeID - UUID of the Datanode.
   * @return True if this is tracked, false if this map does not know about it.
   */
  public boolean isKnownDatanode(UUID datanodeID) {
    Preconditions.checkNotNull(datanodeID);
    return scmNodeStorageReportMap.containsKey(datanodeID);
  }

  public List<UUID> getDatanodeList(
      UtilizationThreshold threshold) {
    return scmNodeStorageReportMap.entrySet().stream().filter(
        entry -> (isThresholdReached(threshold,
            getScmUsedratio(getUsedSpace(entry.getKey()),
                getCapacity(entry.getKey())))))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }



  /**
   * Insert a new datanode into Node2Container Map.
   *
   * @param datanodeID -- Datanode UUID
   * @param report - set if StorageReports.
   */
  public void insertNewDatanode(UUID datanodeID,
      Set<StorageLocationReport> report) throws SCMException {
    Preconditions.checkNotNull(report);
    Preconditions.checkState(report.size() != 0);
    Preconditions.checkNotNull(datanodeID);
    synchronized (scmNodeStorageReportMap) {
      if (isKnownDatanode(datanodeID)) {
        throw new SCMException("Node already exists in the map",
            DUPLICATE_DATANODE);
      }
      scmNodeStorageReportMap.putIfAbsent(datanodeID, report);
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
   * @param report - set of Storage Reports for the Datanode.
   * @throws SCMException - if we don't know about this datanode, for new DN
   *                        use addDatanodeInContainerMap.
   */
  public void updateDatanodeMap(UUID datanodeID,
      Set<StorageLocationReport> report) throws SCMException {
    Preconditions.checkNotNull(datanodeID);
    Preconditions.checkNotNull(report);
    Preconditions.checkState(report.size() != 0);
    synchronized (scmNodeStorageReportMap) {
      if (!scmNodeStorageReportMap.containsKey(datanodeID)) {
        throw new SCMException("No such datanode", NO_SUCH_DATANODE);
      }
      scmNodeStorageReportMap.put(datanodeID, report);
    }
  }

  public StorageReportResult processNodeReport(UUID datanodeID,
      StorageContainerDatanodeProtocolProtos.NodeReportProto nodeReport)
      throws IOException {
    Preconditions.checkNotNull(datanodeID);
    Preconditions.checkNotNull(nodeReport);

    long totalCapacity = 0;
    long totalRemaining = 0;
    long totalScmUsed = 0;
    Set<StorageLocationReport> storagReportSet = new HashSet<>();
    Set<StorageLocationReport> fullVolumeSet = new HashSet<>();
    Set<StorageLocationReport> failedVolumeSet = new HashSet<>();
    List<StorageReportProto>
        storageReports = nodeReport.getStorageReportList();
    for (StorageReportProto report : storageReports) {
      StorageLocationReport storageReport =
          StorageLocationReport.getFromProtobuf(report);
      storagReportSet.add(storageReport);
      if (report.hasFailed() && report.getFailed()) {
        failedVolumeSet.add(storageReport);
      } else if (isThresholdReached(UtilizationThreshold.CRITICAL,
          getScmUsedratio(report.getScmUsed(), report.getCapacity()))) {
        fullVolumeSet.add(storageReport);
      }
      totalCapacity += report.getCapacity();
      totalRemaining += report.getRemaining();
      totalScmUsed += report.getScmUsed();
    }

    if (!isKnownDatanode(datanodeID)) {
      insertNewDatanode(datanodeID, storagReportSet);
    } else {
      updateDatanodeMap(datanodeID, storagReportSet);
    }
    if (isThresholdReached(UtilizationThreshold.CRITICAL,
        getScmUsedratio(totalScmUsed, totalCapacity))) {
      LOG.warn("Datanode {} is out of storage space. Capacity: {}, Used: {}",
          datanodeID, totalCapacity, totalScmUsed);
      return StorageReportResult.ReportResultBuilder.newBuilder()
          .setStatus(ReportStatus.DATANODE_OUT_OF_SPACE)
          .setFullVolumeSet(fullVolumeSet).setFailedVolumeSet(failedVolumeSet)
          .build();
    }
    if (isThresholdReached(UtilizationThreshold.WARN,
        getScmUsedratio(totalScmUsed, totalCapacity))) {
      LOG.warn("Datanode {} is low on storage space. Capacity: {}, Used: {}",
          datanodeID, totalCapacity, totalScmUsed);
    }

    if (failedVolumeSet.isEmpty() && !fullVolumeSet.isEmpty()) {
      return StorageReportResult.ReportResultBuilder.newBuilder()
          .setStatus(ReportStatus.STORAGE_OUT_OF_SPACE)
          .setFullVolumeSet(fullVolumeSet).build();
    }

    if (!failedVolumeSet.isEmpty() && fullVolumeSet.isEmpty()) {
      return StorageReportResult.ReportResultBuilder.newBuilder()
          .setStatus(ReportStatus.FAILED_STORAGE)
          .setFailedVolumeSet(failedVolumeSet).build();
    }
    if (!failedVolumeSet.isEmpty() && !fullVolumeSet.isEmpty()) {
      return StorageReportResult.ReportResultBuilder.newBuilder()
          .setStatus(ReportStatus.FAILED_AND_OUT_OF_SPACE_STORAGE)
          .setFailedVolumeSet(failedVolumeSet).setFullVolumeSet(fullVolumeSet)
          .build();
    }
    return StorageReportResult.ReportResultBuilder.newBuilder()
        .setStatus(ReportStatus.ALL_IS_WELL).build();
  }

  private boolean isThresholdReached(UtilizationThreshold threshold,
      double scmUsedratio) {
    switch (threshold) {
    case NORMAL:
      return scmUsedratio < warningUtilizationThreshold;
    case WARN:
      return scmUsedratio >= warningUtilizationThreshold
          && scmUsedratio < criticalUtilizationThreshold;
    case CRITICAL:
      return scmUsedratio >= criticalUtilizationThreshold;
    default:
      throw new RuntimeException("Unknown UtilizationThreshold value");
    }
  }

  @Override
  public long getCapacity(UUID dnId) {
    long capacity = 0;
    Set<StorageLocationReport> reportSet = scmNodeStorageReportMap.get(dnId);
    for (StorageLocationReport report : reportSet) {
      capacity += report.getCapacity();
    }
    return capacity;
  }

  @Override
  public long getRemainingSpace(UUID dnId) {
    long remaining = 0;
    Set<StorageLocationReport> reportSet = scmNodeStorageReportMap.get(dnId);
    for (StorageLocationReport report : reportSet) {
      remaining += report.getRemaining();
    }
    return remaining;
  }

  @Override
  public long getUsedSpace(UUID dnId) {
    long scmUsed = 0;
    Set<StorageLocationReport> reportSet = scmNodeStorageReportMap.get(dnId);
    for (StorageLocationReport report : reportSet) {
      scmUsed += report.getScmUsed();
    }
    return scmUsed;
  }

  @Override
  public long getTotalCapacity() {
    long capacity = 0;
    Set<UUID> dnIdSet = scmNodeStorageReportMap.keySet();
    for (UUID id : dnIdSet) {
      capacity += getCapacity(id);
    }
    return capacity;
  }

  @Override
  public long getTotalSpaceUsed() {
    long scmUsed = 0;
    Set<UUID> dnIdSet = scmNodeStorageReportMap.keySet();
    for (UUID id : dnIdSet) {
      scmUsed += getUsedSpace(id);
    }
    return scmUsed;
  }

  @Override
  public long getTotalFreeSpace() {
    long remaining = 0;
    Set<UUID> dnIdSet = scmNodeStorageReportMap.keySet();
    for (UUID id : dnIdSet) {
      remaining += getRemainingSpace(id);
    }
    return remaining;
  }

  /**
   * removes the dataNode from scmNodeStorageReportMap.
   * @param datanodeID
   * @throws SCMException in case the dataNode is not found in the map.
   */
  public void removeDatanode(UUID datanodeID) throws SCMException {
    Preconditions.checkNotNull(datanodeID);
    synchronized (scmNodeStorageReportMap) {
      if (!scmNodeStorageReportMap.containsKey(datanodeID)) {
        throw new SCMException("No such datanode", NO_SUCH_DATANODE);
      }
      scmNodeStorageReportMap.remove(datanodeID);
    }
  }

  /**
   * Returns the set of storage volumes for a Datanode.
   * @param  datanodeID
   * @return set of storage volumes.
   */

  @Override
  public Set<StorageLocationReport> getStorageVolumes(UUID datanodeID) {
    return scmNodeStorageReportMap.get(datanodeID);
  }


  /**
   * Truncate to 4 digits since uncontrolled precision is some times
   * counter intuitive to what users expect.
   * @param value - double.
   * @return double.
   */
  private double truncateDecimals(double value) {
    final int multiplier = 10000;
    return (double) ((long) (value * multiplier)) / multiplier;
  }

  /**
   * get the scmUsed ratio.
   */
  public  double getScmUsedratio(long scmUsed, long capacity) {
    double scmUsedRatio =
        truncateDecimals(scmUsed / (double) capacity);
    return scmUsedRatio;
  }
  /**
   * Results possible from processing a Node report by
   * Node2ContainerMapper.
   */
  public enum ReportStatus {
    ALL_IS_WELL,
    DATANODE_OUT_OF_SPACE,
    STORAGE_OUT_OF_SPACE,
    FAILED_STORAGE,
    FAILED_AND_OUT_OF_SPACE_STORAGE
  }

}