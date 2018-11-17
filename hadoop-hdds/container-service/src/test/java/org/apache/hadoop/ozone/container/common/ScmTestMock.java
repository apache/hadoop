/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.scm.VersionInfo;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.VersionResponse;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SCM RPC mock class.
 */
public class ScmTestMock implements StorageContainerDatanodeProtocol {
  private int rpcResponseDelay;
  private AtomicInteger heartbeatCount = new AtomicInteger(0);
  private AtomicInteger rpcCount = new AtomicInteger(0);
  private AtomicInteger containerReportsCount = new AtomicInteger(0);
  private String clusterId;
  private String scmId;

  public ScmTestMock() {
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
  }

  // Map of datanode to containers
  private Map<DatanodeDetails,
      Map<String, ContainerReplicaProto>> nodeContainers =
      new HashMap<>();
  private Map<DatanodeDetails, NodeReportProto> nodeReports = new HashMap<>();
  private AtomicInteger commandStatusReport = new AtomicInteger(0);
  private List<CommandStatus> cmdStatusList = new ArrayList<>();
  private List<SCMCommandProto> scmCommandRequests = new ArrayList<>();
  /**
   * Returns the number of heartbeats made to this class.
   *
   * @return int
   */
  public int getHeartbeatCount() {
    return heartbeatCount.get();
  }

  /**
   * Returns the number of RPC calls made to this mock class instance.
   *
   * @return - Number of RPC calls serviced by this class.
   */
  public int getRpcCount() {
    return rpcCount.get();
  }

  /**
   * Gets the RPC response delay.
   *
   * @return delay in milliseconds.
   */
  public int getRpcResponseDelay() {
    return rpcResponseDelay;
  }

  /**
   * Sets the RPC response delay.
   *
   * @param rpcResponseDelay - delay in milliseconds.
   */
  public void setRpcResponseDelay(int rpcResponseDelay) {
    this.rpcResponseDelay = rpcResponseDelay;
  }

  /**
   * Returns the number of container reports server has seen.
   * @return int
   */
  public int getContainerReportsCount() {
    return containerReportsCount.get();
  }

  /**
   * Returns the number of containers that have been reported so far.
   * @return - count of reported containers.
   */
  public long getContainerCount() {
    return nodeContainers.values().parallelStream().mapToLong((containerMap)->{
      return containerMap.size();
    }).sum();
  }

  /**
   * Get the number keys reported from container reports.
   * @return - number of keys reported.
   */
  public long getKeyCount() {
    return nodeContainers.values().parallelStream().mapToLong((containerMap)->{
      return containerMap.values().parallelStream().mapToLong((container) -> {
        return container.getKeyCount();
      }).sum();
    }).sum();
  }

  /**
   * Get the number of bytes used from container reports.
   * @return - number of bytes used.
   */
  public long getBytesUsed() {
    return nodeContainers.values().parallelStream().mapToLong((containerMap)->{
      return containerMap.values().parallelStream().mapToLong((container) -> {
        return container.getUsed();
      }).sum();
    }).sum();
  }

  /**
   * Returns SCM version.
   *
   * @return Version info.
   */
  @Override
  public StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto
      getVersion(StorageContainerDatanodeProtocolProtos
      .SCMVersionRequestProto unused) throws IOException {
    rpcCount.incrementAndGet();
    sleepIfNeeded();
    VersionInfo versionInfo = VersionInfo.getLatestVersion();
    return VersionResponse.newBuilder()
        .setVersion(versionInfo.getVersion())
        .addValue(VersionInfo.DESCRIPTION_KEY, versionInfo.getDescription())
        .addValue(OzoneConsts.SCM_ID, scmId)
        .addValue(OzoneConsts.CLUSTER_ID, clusterId)
        .build().getProtobufMessage();

  }

  private void sleepIfNeeded() {
    if (getRpcResponseDelay() > 0) {
      try {
        Thread.sleep(getRpcResponseDelay());
      } catch (InterruptedException ex) {
        // Just ignore this exception.
      }
    }
  }

  /**
   * Used by data node to send a Heartbeat.
   *
   * @param heartbeat - node heartbeat.
   * @return - SCMHeartbeatResponseProto
   * @throws IOException
   */
  @Override
  public StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto
      sendHeartbeat(SCMHeartbeatRequestProto heartbeat) throws IOException {
    rpcCount.incrementAndGet();
    heartbeatCount.incrementAndGet();
    if (heartbeat.getCommandStatusReportsCount() != 0) {
      for (CommandStatusReportsProto statusReport : heartbeat
          .getCommandStatusReportsList()) {
        cmdStatusList.addAll(statusReport.getCmdStatusList());
        commandStatusReport.incrementAndGet();
      }
    }
    sleepIfNeeded();
    return SCMHeartbeatResponseProto.newBuilder().addAllCommands(
        scmCommandRequests)
        .setDatanodeUUID(heartbeat.getDatanodeDetails().getUuid())
        .build();
  }

  /**
   * Register Datanode.
   *
   * @param datanodeDetailsProto DatanodDetailsProto.
   * @return SCM Command.
   */
  @Override
  public StorageContainerDatanodeProtocolProtos
      .SCMRegisteredResponseProto register(
          DatanodeDetailsProto datanodeDetailsProto, NodeReportProto nodeReport,
          ContainerReportsProto containerReportsRequestProto,
          PipelineReportsProto pipelineReportsProto)
      throws IOException {
    rpcCount.incrementAndGet();
    updateNodeReport(datanodeDetailsProto, nodeReport);
    updateContainerReport(containerReportsRequestProto, datanodeDetailsProto);
    sleepIfNeeded();
    return StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto
        .newBuilder().setClusterID(UUID.randomUUID().toString())
        .setDatanodeUUID(datanodeDetailsProto.getUuid()).setErrorCode(
            StorageContainerDatanodeProtocolProtos
                .SCMRegisteredResponseProto.ErrorCode.success).build();
  }

  /**
   * Update nodeReport.
   * @param datanodeDetailsProto
   * @param nodeReport
   */
  public void updateNodeReport(DatanodeDetailsProto datanodeDetailsProto,
      NodeReportProto nodeReport) {
    DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(
        datanodeDetailsProto);
    NodeReportProto.Builder nodeReportProto = NodeReportProto.newBuilder();

    List<StorageReportProto> storageReports =
        nodeReport.getStorageReportList();

    for(StorageReportProto report : storageReports) {
      nodeReportProto.addStorageReport(report);
    }

    nodeReports.put(datanode, nodeReportProto.build());

  }

  /**
   * Update the cotainerReport.
   *
   * @param reports Container report
   * @param datanodeDetails DataNode Info
   * @throws IOException
   */
  public void updateContainerReport(
      StorageContainerDatanodeProtocolProtos.ContainerReportsProto reports,
      DatanodeDetailsProto datanodeDetails) throws IOException {
    Preconditions.checkNotNull(reports);
    containerReportsCount.incrementAndGet();
    DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(
        datanodeDetails);
    if (reports.getReportsCount() > 0) {
      Map containers = nodeContainers.get(datanode);
      if (containers == null) {
        containers = new LinkedHashMap();
        nodeContainers.put(datanode, containers);
      }

      for (ContainerReplicaProto report : reports
          .getReportsList()) {
        containers.put(report.getContainerID(), report);
      }
    }
  }


  /**
   * Return the number of StorageReports of a datanode.
   * @param datanodeDetails
   * @return count of containers of a datanode
   */
  public int getNodeReportsCount(DatanodeDetails datanodeDetails) {
    return nodeReports.get(datanodeDetails).getStorageReportCount();
  }

  /**
   * Returns the number of containers of a datanode.
   * @param datanodeDetails
   * @return count of storage reports of a datanode
   */
  public int getContainerCountsForDatanode(DatanodeDetails datanodeDetails) {
    Map<String, ContainerReplicaProto> cr =
        nodeContainers.get(datanodeDetails);
    if(cr != null) {
      return cr.size();
    }
    return 0;
  }

  /**
   * Reset the mock Scm for test to get a fresh start without rebuild MockScm.
   */
  public void reset() {
    heartbeatCount.set(0);
    rpcCount.set(0);
    containerReportsCount.set(0);
    nodeContainers.clear();

  }

  public int getCommandStatusReportCount() {
    return commandStatusReport.get();
  }

  public List<CommandStatus> getCmdStatusList() {
    return cmdStatusList;
  }

  public List<SCMCommandProto> getScmCommandRequests() {
    return scmCommandRequests;
  }

  public void clearScmCommandRequests() {
    scmCommandRequests.clear();
  }

  public void addScmCommandRequest(SCMCommandProto scmCmd) {
    scmCommandRequests.add(scmCmd);
  }

  /**
   * Set scmId.
   * @param id
   */
  public void setScmId(String id) {
    this.scmId = id;
  }

  /**
   * Set scmId.
   * @return scmId
   */
  public String getScmId() {
    return scmId;
  }
}
