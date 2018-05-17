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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredCmdResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SendContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMReregisterCmdResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCmdType;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;


import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCmdType.versionCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCmdType.registeredCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCmdType.sendContainerReport;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCmdType.reregisterCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCmdType.deleteBlocksCommand;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCmdType.closeContainerCommand;


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
      HddsProtos.DatanodeDetailsProto datanodeDetails,
      StorageContainerDatanodeProtocolProtos.SCMNodeReport nodeReport,
      StorageContainerDatanodeProtocolProtos.ReportState reportState)
      throws IOException {
    List<SCMCommand> commands =
        scm.getScmNodeManager().sendHeartbeat(datanodeDetails, nodeReport,
            reportState);
    List<SCMCommandResponseProto> cmdResponses = new LinkedList<>();
    for (SCMCommand cmd : commands) {
      cmdResponses.add(getCommandResponse(cmd, datanodeDetails.getUuid()));
    }
    return SCMHeartbeatResponseProto.newBuilder()
        .addAllCommands(cmdResponses).build();
  }

  @Override
  public SCMRegisteredCmdResponseProto register(
      HddsProtos.DatanodeDetailsProto datanodeDetails, SCMNodeReport nodeReport,
      ContainerReportsRequestProto containerReportsRequestProto)
      throws IOException {
    // TODO : Return the list of Nodes that forms the SCM HA.
    RegisteredCommand registeredCommand = (RegisteredCommand) scm
        .getScmNodeManager().register(datanodeDetails, nodeReport);
    SCMCmdType type = registeredCommand.getType();
    if (type == SCMCmdType.registeredCommand && registeredCommand.getError()
        == SCMRegisteredCmdResponseProto.ErrorCode.success) {
      scm.getScmContainerManager().processContainerReports(
          containerReportsRequestProto);
    }
    return getRegisteredResponse(registeredCommand);
  }

  @VisibleForTesting
  public static SCMRegisteredCmdResponseProto getRegisteredResponse(
        SCMCommand cmd) {
    Preconditions.checkState(cmd.getClass() == RegisteredCommand.class);
    RegisteredCommand rCmd = (RegisteredCommand) cmd;
    SCMCmdType type = cmd.getType();
    if (type != SCMCmdType.registeredCommand) {
      throw new IllegalArgumentException(
          "Registered command is not well " + "formed. Internal Error.");
    }
    return SCMRegisteredCmdResponseProto.newBuilder()
        // TODO : Fix this later when we have multiple SCM support.
        // .setAddressList(addressList)
        .setErrorCode(rCmd.getError())
        .setClusterID(rCmd.getClusterID())
        .setDatanodeUUID(rCmd.getDatanodeUUID())
        .build();
  }

  @Override
  public ContainerReportsResponseProto sendContainerReport(
      ContainerReportsRequestProto reports)
      throws IOException {
    updateContainerReportMetrics(reports);

    // should we process container reports async?
    scm.getScmContainerManager().processContainerReports(reports);
    return ContainerReportsResponseProto.newBuilder().build();
  }

  private void updateContainerReportMetrics(
      ContainerReportsRequestProto reports) {
    ContainerStat newStat = null;
    // TODO: We should update the logic once incremental container report
    // type is supported.
    if (reports
        .getType() == StorageContainerDatanodeProtocolProtos
        .ContainerReportsRequestProto.reportType.fullReport) {
      newStat = new ContainerStat();
      for (StorageContainerDatanodeProtocolProtos.ContainerInfo info : reports
          .getReportsList()) {
        newStat.add(new ContainerStat(info.getSize(), info.getUsed(),
            info.getKeyCount(), info.getReadBytes(), info.getWriteBytes(),
            info.getReadCount(), info.getWriteCount()));
      }

      // update container metrics
      StorageContainerManager.getMetrics().setLastContainerStat(newStat);
    }

    // Update container stat entry, this will trigger a removal operation if it
    // exists in cache.
    synchronized (scm.getContainerReportCache()) {
      String datanodeUuid = reports.getDatanodeDetails().getUuid();
      if (datanodeUuid != null && newStat != null) {
        scm.getContainerReportCache().put(datanodeUuid, newStat);
        // update global view container metrics
        StorageContainerManager.getMetrics().incrContainerStat(newStat);
      }
    }
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
  public StorageContainerDatanodeProtocolProtos.SCMCommandResponseProto
      getCommandResponse(
      SCMCommand cmd, final String datanodeID) throws IOException {
    SCMCmdType type = cmd.getType();
    SCMCommandResponseProto.Builder builder =
        SCMCommandResponseProto.newBuilder().setDatanodeUUID(datanodeID);
    switch (type) {
    case registeredCommand:
      return builder
          .setCmdType(registeredCommand)
          .setRegisteredProto(SCMRegisteredCmdResponseProto
              .getDefaultInstance())
          .build();
    case versionCommand:
      return builder
          .setCmdType(versionCommand)
          .setVersionProto(SCMVersionResponseProto.getDefaultInstance())
          .build();
    case sendContainerReport:
      return builder
          .setCmdType(sendContainerReport)
          .setSendReport(SendContainerReportProto.getDefaultInstance())
          .build();
    case reregisterCommand:
      return builder
          .setCmdType(reregisterCommand)
          .setReregisterProto(SCMReregisterCmdResponseProto
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
          .setCmdType(deleteBlocksCommand)
          .setDeleteBlocksProto(((DeleteBlocksCommand) cmd).getProto())
          .build();
    case closeContainerCommand:
      return builder
          .setCmdType(closeContainerCommand)
          .setCloseContainerProto(((CloseContainerCommand) cmd).getProto())
          .build();
    default:
      throw new IllegalArgumentException("Not implemented");
    }
  }
}
