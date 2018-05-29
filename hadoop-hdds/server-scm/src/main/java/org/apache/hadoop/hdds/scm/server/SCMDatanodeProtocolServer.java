/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ReregisterCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos
    .ContainerBlocksDeletionACKResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos
    .ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;


import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto
    .Type.closeContainerCommand;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto
    .Type.deleteBlocksCommand;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto
    .Type.reregisterCommand;



import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.container.placement.metrics.ContainerStat;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB
    .StorageContainerDatanodeProtocolServerSideTranslatorPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;

import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.startRpcServer;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;

/**
 * Protocol Handler for Datanode Protocol.
 */
public class SCMDatanodeProtocolServer implements
    StorageContainerDatanodeProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(
      SCMDatanodeProtocolServer.class);

  /**
   * The RPC server that listens to requests from DataNodes.
   */
  private final RPC.Server datanodeRpcServer;

  private final StorageContainerManager scm;
  private final InetSocketAddress datanodeRpcAddress;

  public SCMDatanodeProtocolServer(final OzoneConfiguration conf,
      StorageContainerManager scm)  throws IOException {

    Preconditions.checkNotNull(scm, "SCM cannot be null");
    this.scm = scm;
    final int handlerCount =
        conf.getInt(OZONE_SCM_HANDLER_COUNT_KEY,
            OZONE_SCM_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(conf, StorageContainerDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    BlockingService dnProtoPbService =
        StorageContainerDatanodeProtocolProtos
            .StorageContainerDatanodeProtocolService
            .newReflectiveBlockingService(
                new StorageContainerDatanodeProtocolServerSideTranslatorPB(
                    this));

    InetSocketAddress datanodeRpcAddr =
        HddsServerUtil.getScmDataNodeBindAddress(conf);

    datanodeRpcServer =
        startRpcServer(
            conf,
            datanodeRpcAddr,
            StorageContainerDatanodeProtocolPB.class,
            dnProtoPbService,
            handlerCount);

    datanodeRpcAddress =
        updateRPCListenAddress(
            conf, OZONE_SCM_DATANODE_ADDRESS_KEY, datanodeRpcAddr,
            datanodeRpcServer);
  }

  public InetSocketAddress getDatanodeRpcAddress() {
    return datanodeRpcAddress;
  }

  public RPC.Server getDatanodeRpcServer() {
    return datanodeRpcServer;
  }

  @Override
  public SCMVersionResponseProto getVersion(SCMVersionRequestProto
      versionRequest)
      throws IOException {
    return scm.getScmNodeManager().getVersion(versionRequest)
        .getProtobufMessage();
  }

  @Override
  public SCMHeartbeatResponseProto sendHeartbeat(
      SCMHeartbeatRequestProto heartbeat)
      throws IOException {
    // TODO: Add a heartbeat dispatcher.
    DatanodeDetails datanodeDetails = DatanodeDetails
        .getFromProtoBuf(heartbeat.getDatanodeDetails());
    NodeReportProto nodeReport = heartbeat.getNodeReport();
    List<SCMCommand> commands =
        scm.getScmNodeManager().sendHeartbeat(datanodeDetails, nodeReport);
    List<SCMCommandProto> cmdResponses = new LinkedList<>();
    for (SCMCommand cmd : commands) {
      cmdResponses.add(getCommandResponse(cmd));
    }
    return SCMHeartbeatResponseProto.newBuilder()
        .setDatanodeUUID(datanodeDetails.getUuidString())
        .addAllCommands(cmdResponses).build();
  }

  @Override
  public SCMRegisteredResponseProto register(
      HddsProtos.DatanodeDetailsProto datanodeDetailsProto,
      NodeReportProto nodeReport,
      ContainerReportsProto containerReportsProto)
      throws IOException {
    DatanodeDetails datanodeDetails = DatanodeDetails
        .getFromProtoBuf(datanodeDetailsProto);
    // TODO : Return the list of Nodes that forms the SCM HA.
    RegisteredCommand registeredCommand = scm.getScmNodeManager()
        .register(datanodeDetails, nodeReport);
    if (registeredCommand.getError()
        == SCMRegisteredResponseProto.ErrorCode.success) {
      scm.getScmContainerManager().processContainerReports(datanodeDetails,
          containerReportsProto);
    }
    return getRegisteredResponse(registeredCommand);
  }

  @VisibleForTesting
  public static SCMRegisteredResponseProto getRegisteredResponse(
      RegisteredCommand cmd) {
    return SCMRegisteredResponseProto.newBuilder()
        // TODO : Fix this later when we have multiple SCM support.
        // .setAddressList(addressList)
        .setErrorCode(cmd.getError())
        .setClusterID(cmd.getClusterID())
        .setDatanodeUUID(cmd.getDatanodeUUID())
        .build();
  }

  public void processContainerReports(DatanodeDetails datanodeDetails,
                                      ContainerReportsProto reports)
      throws IOException {
    updateContainerReportMetrics(datanodeDetails, reports);
    // should we process container reports async?
    scm.getScmContainerManager()
        .processContainerReports(datanodeDetails, reports);
  }

  private void updateContainerReportMetrics(DatanodeDetails datanodeDetails,
                                            ContainerReportsProto reports) {
    ContainerStat newStat = new ContainerStat();
    for (StorageContainerDatanodeProtocolProtos.ContainerInfo info : reports
        .getReportsList()) {
      newStat.add(new ContainerStat(info.getSize(), info.getUsed(),
          info.getKeyCount(), info.getReadBytes(), info.getWriteBytes(),
          info.getReadCount(), info.getWriteCount()));
    }
    // update container metrics
    StorageContainerManager.getMetrics().setLastContainerStat(newStat);

    // Update container stat entry, this will trigger a removal operation if it
    // exists in cache.
    String datanodeUuid = datanodeDetails.getUuidString();
    scm.getContainerReportCache().put(datanodeUuid, newStat);
    // update global view container metrics
    StorageContainerManager.getMetrics().incrContainerStat(newStat);
  }


  @Override
  public ContainerBlocksDeletionACKResponseProto sendContainerBlocksDeletionACK(
      ContainerBlocksDeletionACKProto acks) throws IOException {
    if (acks.getResultsCount() > 0) {
      List<DeleteBlockTransactionResult> resultList = acks.getResultsList();
      for (DeleteBlockTransactionResult result : resultList) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got block deletion ACK from datanode, TXIDs={}, "
              + "success={}", result.getTxID(), result.getSuccess());
        }
        if (result.getSuccess()) {
          LOG.debug("Purging TXID={} from block deletion log",
              result.getTxID());
          scm.getScmBlockManager().getDeletedBlockLog()
              .commitTransactions(Collections.singletonList(result.getTxID()));
        } else {
          LOG.warn("Got failed ACK for TXID={}, prepare to resend the "
              + "TX in next interval", result.getTxID());
        }
      }
    }
    return ContainerBlocksDeletionACKResponseProto.newBuilder()
        .getDefaultInstanceForType();
  }

  public void start() {
    LOG.info(
        StorageContainerManager.buildRpcServerStartMessage(
            "RPC server for DataNodes", getDatanodeRpcAddress()));
    getDatanodeRpcServer().start();
  }

  public void stop() {
    try {
      LOG.info("Stopping the RPC server for DataNodes");
      datanodeRpcServer.stop();
    } catch (Exception ex) {
      LOG.error(" datanodeRpcServer stop failed.", ex);
    }
    IOUtils.cleanupWithLogger(LOG, scm.getScmNodeManager());
  }

  public void join() throws InterruptedException {
    LOG.trace("Join RPC server for DataNodes");
    datanodeRpcServer.join();
  }

  /**
   * Returns a SCMCommandRepose from the SCM Command.
   *
   * @param cmd - Cmd
   * @return SCMCommandResponseProto
   * @throws IOException
   */
  @VisibleForTesting
  public SCMCommandProto getCommandResponse(SCMCommand cmd)
      throws IOException {
    SCMCommandProto.Builder builder =
        SCMCommandProto.newBuilder();
    switch (cmd.getType()) {
    case reregisterCommand:
      return builder
          .setCommandType(reregisterCommand)
          .setReregisterCommandProto(ReregisterCommandProto
              .getDefaultInstance())
          .build();
    case deleteBlocksCommand:
      // Once SCM sends out the deletion message, increment the count.
      // this is done here instead of when SCM receives the ACK, because
      // DN might not be able to response the ACK for sometime. In case
      // it times out, SCM needs to re-send the message some more times.
      List<Long> txs =
          ((DeleteBlocksCommand) cmd)
              .blocksTobeDeleted()
              .stream()
              .map(tx -> tx.getTxID())
              .collect(Collectors.toList());
      scm.getScmBlockManager().getDeletedBlockLog().incrementCount(txs);
      return builder
          .setCommandType(deleteBlocksCommand)
          .setDeleteBlocksCommandProto(((DeleteBlocksCommand) cmd).getProto())
          .build();
    case closeContainerCommand:
      return builder
          .setCommandType(closeContainerCommand)
          .setCloseContainerCommandProto(
              ((CloseContainerCommand) cmd).getProto())
          .build();
    default:
      throw new IllegalArgumentException("Not implemented");
    }
  }
}
