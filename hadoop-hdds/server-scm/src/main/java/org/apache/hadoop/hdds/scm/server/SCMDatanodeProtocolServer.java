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
import com.google.common.collect.Maps;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;

import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
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

import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto
    .Type.closeContainerCommand;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto
    .Type.deleteBlocksCommand;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto
    .Type.deleteContainerCommand;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type
    .replicateContainerCommand;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto
    .Type.reregisterCommand;



import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .ReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
        .PipelineReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.SCMAction;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB
    .StorageContainerDatanodeProtocolServerSideTranslatorPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;

import static org.apache.hadoop.hdds.scm.events.SCMEvents.CONTAINER_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.PIPELINE_REPORT;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.startRpcServer;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;

/**
 * Protocol Handler for Datanode Protocol.
 */
public class SCMDatanodeProtocolServer implements
    StorageContainerDatanodeProtocol, Auditor {

  private static final Logger LOG = LoggerFactory.getLogger(
      SCMDatanodeProtocolServer.class);

  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.SCMLOGGER);

  /**
   * The RPC server that listens to requests from DataNodes.
   */
  private final RPC.Server datanodeRpcServer;

  private final StorageContainerManager scm;
  private final InetSocketAddress datanodeRpcAddress;
  private final SCMDatanodeHeartbeatDispatcher heartbeatDispatcher;
  private final EventPublisher eventPublisher;

  public SCMDatanodeProtocolServer(final OzoneConfiguration conf,
      StorageContainerManager scm, EventPublisher eventPublisher)
      throws IOException {

    Preconditions.checkNotNull(scm, "SCM cannot be null");
    Preconditions.checkNotNull(eventPublisher, "EventPublisher cannot be null");

    this.scm = scm;
    this.eventPublisher = eventPublisher;
    final int handlerCount =
        conf.getInt(OZONE_SCM_HANDLER_COUNT_KEY,
            OZONE_SCM_HANDLER_COUNT_DEFAULT);

    heartbeatDispatcher = new SCMDatanodeHeartbeatDispatcher(
        scm.getScmNodeManager(), eventPublisher);

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

    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      datanodeRpcServer.refreshServiceAcl(conf,
          SCMPolicyProvider.getInstance());
    }
  }

  public void start() {
    LOG.info(
        StorageContainerManager.buildRpcServerStartMessage(
            "RPC server for DataNodes", datanodeRpcAddress));
    datanodeRpcServer.start();
  }

  public InetSocketAddress getDatanodeRpcAddress() {
    return datanodeRpcAddress;
  }

  @Override
  public SCMVersionResponseProto getVersion(SCMVersionRequestProto
      versionRequest)
      throws IOException {
    boolean auditSuccess = true;
    try {
      return scm.getScmNodeManager().getVersion(versionRequest)
              .getProtobufMessage();
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.GET_VERSION, null, ex));
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.GET_VERSION, null));
      }
    }
  }

  @Override
  public SCMRegisteredResponseProto register(
      HddsProtos.DatanodeDetailsProto datanodeDetailsProto,
      NodeReportProto nodeReport,
      ContainerReportsProto containerReportsProto,
          PipelineReportsProto pipelineReportsProto)
      throws IOException {
    DatanodeDetails datanodeDetails = DatanodeDetails
        .getFromProtoBuf(datanodeDetailsProto);
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("datanodeDetails", datanodeDetails.toString());

    // TODO : Return the list of Nodes that forms the SCM HA.
    RegisteredCommand registeredCommand = scm.getScmNodeManager()
        .register(datanodeDetails, nodeReport, pipelineReportsProto);
    if (registeredCommand.getError()
        == SCMRegisteredResponseProto.ErrorCode.success) {
      eventPublisher.fireEvent(CONTAINER_REPORT,
          new SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode(
              datanodeDetails, containerReportsProto));
      eventPublisher.fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
          new NodeRegistrationContainerReport(datanodeDetails,
              containerReportsProto));
      eventPublisher.fireEvent(PIPELINE_REPORT,
              new PipelineReportFromDatanode(datanodeDetails,
                      pipelineReportsProto));
    }
    try {
      return getRegisteredResponse(registeredCommand);
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(SCMAction.REGISTER, auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(SCMAction.REGISTER, auditMap));
      }
    }
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

  @Override
  public SCMHeartbeatResponseProto sendHeartbeat(
      SCMHeartbeatRequestProto heartbeat) throws IOException {
    List<SCMCommandProto> cmdResponses = new ArrayList<>();
    for (SCMCommand cmd : heartbeatDispatcher.dispatch(heartbeat)) {
      cmdResponses.add(getCommandResponse(cmd));
    }
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("datanodeUUID", heartbeat.getDatanodeDetails().getUuid());
    auditMap.put("command", flatten(cmdResponses.toString()));
    try {
      return SCMHeartbeatResponseProto.newBuilder()
          .setDatanodeUUID(heartbeat.getDatanodeDetails().getUuid())
          .addAllCommands(cmdResponses).build();
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(SCMAction.SEND_HEARTBEAT, auditMap, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(SCMAction.SEND_HEARTBEAT, auditMap)
        );
      }
    }
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
    case deleteContainerCommand:
      return builder.setCommandType(deleteContainerCommand)
          .setDeleteContainerCommandProto(
              ((DeleteContainerCommand) cmd).getProto())
          .build();
    case replicateContainerCommand:
      return builder
          .setCommandType(replicateContainerCommand)
          .setReplicateContainerCommandProto(
              ((ReplicateContainerCommand)cmd).getProto())
          .build();
    default:
      throw new IllegalArgumentException("Scm command " +
          cmd.getType().toString() + " is not implemented");
    }
  }


  public void join() throws InterruptedException {
    LOG.trace("Join RPC server for DataNodes");
    datanodeRpcServer.join();
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

  @Override
  public AuditMessage buildAuditMessageForSuccess(
      AuditAction op, Map<String, String> auditMap) {
    return new AuditMessage.Builder()
        .setUser((Server.getRemoteUser() == null) ? null :
            Server.getRemoteUser().getUserName())
        .atIp((Server.getRemoteIp() == null) ? null :
            Server.getRemoteIp().getHostAddress())
        .forOperation(op.getAction())
        .withParams(auditMap)
        .withResult(AuditEventStatus.SUCCESS.toString())
        .withException(null)
        .build();
  }

  @Override
  public AuditMessage buildAuditMessageForFailure(AuditAction op, Map<String,
      String> auditMap, Throwable throwable) {
    return new AuditMessage.Builder()
        .setUser((Server.getRemoteUser() == null) ? null :
            Server.getRemoteUser().getUserName())
        .atIp((Server.getRemoteIp() == null) ? null :
            Server.getRemoteIp().getHostAddress())
        .forOperation(op.getAction())
        .withParams(auditMap)
        .withResult(AuditEventStatus.FAILURE.toString())
        .withException(throwable)
        .build();
  }

  private static String flatten(String input) {
    return input
        .replaceAll(System.lineSeparator(), " ")
        .trim()
        .replaceAll(" +", " ");
  }

  /**
   * Wrapper class for events with the datanode origin.
   */
  public static class NodeRegistrationContainerReport extends
      ReportFromDatanode<ContainerReportsProto> {

    public NodeRegistrationContainerReport(DatanodeDetails datanodeDetails,
        ContainerReportsProto report) {
      super(datanodeDetails, report);
    }
  }

}
