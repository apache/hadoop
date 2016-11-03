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

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.NullCommand;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.scm.VersionInfo;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SCM RPC mock class.
 */
public class ScmTestMock implements StorageContainerDatanodeProtocol {
  private int rpcResponseDelay;
  private AtomicInteger heartbeatCount = new AtomicInteger(0);
  private AtomicInteger rpcCount = new AtomicInteger(0);

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
   * Returns SCM version.
   *
   * @return Version info.
   */
  @Override
  public StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto
      getVersion() throws IOException {
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
   * @return - SCMHeartbeatResponseProto
   * @throws IOException
   */
  @Override
  public StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto
      sendHeartbeat(DatanodeID datanodeID)
      throws IOException {
    rpcCount.incrementAndGet();
    heartbeatCount.incrementAndGet();
    sleepIfNeeded();
    StorageContainerDatanodeProtocolProtos.SCMCommandResponseProto
        cmdResponse = StorageContainerDatanodeProtocolProtos
        .SCMCommandResponseProto
        .newBuilder().setCmdType(StorageContainerDatanodeProtocolProtos
            .Type.nullCmd)
        .setNullCommand(
            NullCommand.newBuilder().build().getProtoBufMessage()).build();
    return StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto
        .newBuilder()
        .addCommands(cmdResponse).build();
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
}
