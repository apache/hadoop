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
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerInfo;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ReportState;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKResponseProto;
import org.apache.hadoop.ozone.scm.VersionInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SCM RPC mock class.
 */
public class ScmTestMock implements StorageContainerDatanodeProtocol {
  private int rpcResponseDelay;
  private AtomicInteger heartbeatCount = new AtomicInteger(0);
  private AtomicInteger rpcCount = new AtomicInteger(0);
  private ReportState reportState;
  private AtomicInteger containerReportsCount = new AtomicInteger(0);

  // Map of datanode to containers
  private Map<DatanodeID, Map<String, ContainerInfo>> nodeContainers =
      new HashMap();
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
   * @param datanodeID - Datanode ID.
   * @param nodeReport - node report.
   * @return - SCMHeartbeatResponseProto
   * @throws IOException
   */
  @Override
  public StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto
      sendHeartbeat(DatanodeID datanodeID, SCMNodeReport nodeReport,
      ReportState scmReportState) throws IOException {
    rpcCount.incrementAndGet();
    heartbeatCount.incrementAndGet();
    this.reportState = scmReportState;
    sleepIfNeeded();
    List<SCMCommandResponseProto>
        cmdResponses = new LinkedList<>();
    return SCMHeartbeatResponseProto.newBuilder().addAllCommands(cmdResponses)
        .build();
  }

  /**
   * Register Datanode.
   *
   * @param datanodeID - DatanodID.
   * @param scmAddresses - List of SCMs this datanode is configured to
   * communicate.
   * @return SCM Command.
   */
  @Override
  public StorageContainerDatanodeProtocolProtos
      .SCMRegisteredCmdResponseProto register(DatanodeID datanodeID,
      String[] scmAddresses) throws IOException {
    rpcCount.incrementAndGet();
    sleepIfNeeded();
    return StorageContainerDatanodeProtocolProtos
        .SCMRegisteredCmdResponseProto
        .newBuilder().setClusterID(UUID.randomUUID().toString())
        .setDatanodeUUID(datanodeID.getDatanodeUuid()).setErrorCode(
            StorageContainerDatanodeProtocolProtos
                .SCMRegisteredCmdResponseProto.ErrorCode.success).build();
  }

  /**
   * Send a container report.
   *
   * @param reports -- Container report
   * @return HeartbeatResponse.nullcommand.
   * @throws IOException
   */
  @Override
  public StorageContainerDatanodeProtocolProtos.ContainerReportsResponseProto
      sendContainerReport(StorageContainerDatanodeProtocolProtos
      .ContainerReportsRequestProto reports) throws IOException {
    Preconditions.checkNotNull(reports);
    containerReportsCount.incrementAndGet();

    DatanodeID datanode = DatanodeID.getFromProtoBuf(reports.getDatanodeID());
    if (reports.getReportsCount() > 0) {
      Map containers = nodeContainers.get(datanode);
      if (containers == null) {
        containers = new LinkedHashMap();
        nodeContainers.put(datanode, containers);
      }

      for (StorageContainerDatanodeProtocolProtos.ContainerInfo report:
          reports.getReportsList()) {
        containers.put(report.getContainerName(), report);
      }
    }

    return StorageContainerDatanodeProtocolProtos
        .ContainerReportsResponseProto.newBuilder().build();
  }

  @Override
  public ContainerBlocksDeletionACKResponseProto sendContainerBlocksDeletionACK(
      ContainerBlocksDeletionACKProto request) throws IOException {
    return ContainerBlocksDeletionACKResponseProto
        .newBuilder().getDefaultInstanceForType();
  }

  public ReportState getReportState() {
    return this.reportState;
  }

  /**
   * Reset the mock Scm for test to get a fresh start without rebuild MockScm.
   */
  public void reset() {
    heartbeatCount.set(0);
    rpcCount.set(0);
    reportState = ReportState.newBuilder()
        .setState(ReportState.states.noContainerReports)
        .setCount(0).build();
    containerReportsCount.set(0);
    nodeContainers.clear();

  }
}
