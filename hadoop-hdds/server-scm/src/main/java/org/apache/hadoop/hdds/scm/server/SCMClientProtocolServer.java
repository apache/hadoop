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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmOps;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.safemode.SafeModePrecheck;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
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
import org.apache.hadoop.ozone.protocolPB
    .StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos
    .StorageContainerLocationProtocolService.newReflectiveBlockingService;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_CLIENT_ADDRESS_KEY;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager
    .startRpcServer;

/**
 * The RPC server that listens to requests from clients.
 */
public class SCMClientProtocolServer implements
    StorageContainerLocationProtocol, Auditor {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMClientProtocolServer.class);
  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.SCMLOGGER);
  private final RPC.Server clientRpcServer;
  private final InetSocketAddress clientRpcAddress;
  private final StorageContainerManager scm;
  private final OzoneConfiguration conf;
  private SafeModePrecheck safeModePrecheck;

  public SCMClientProtocolServer(OzoneConfiguration conf,
      StorageContainerManager scm) throws IOException {
    this.scm = scm;
    this.conf = conf;
    safeModePrecheck = new SafeModePrecheck(conf);
    final int handlerCount =
        conf.getInt(OZONE_SCM_HANDLER_COUNT_KEY,
            OZONE_SCM_HANDLER_COUNT_DEFAULT);
    RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);

    // SCM Container Service RPC
    BlockingService storageProtoPbService =
        newReflectiveBlockingService(
            new StorageContainerLocationProtocolServerSideTranslatorPB(this));

    final InetSocketAddress scmAddress = HddsServerUtil
        .getScmClientBindAddress(conf);
    clientRpcServer =
        startRpcServer(
            conf,
            scmAddress,
            StorageContainerLocationProtocolPB.class,
            storageProtoPbService,
            handlerCount);
    clientRpcAddress =
        updateRPCListenAddress(conf, OZONE_SCM_CLIENT_ADDRESS_KEY,
            scmAddress, clientRpcServer);
    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      clientRpcServer.refreshServiceAcl(conf, SCMPolicyProvider.getInstance());
    }
  }

  public RPC.Server getClientRpcServer() {
    return clientRpcServer;
  }

  public InetSocketAddress getClientRpcAddress() {
    return clientRpcAddress;
  }

  public void start() {
    LOG.info(
        StorageContainerManager.buildRpcServerStartMessage(
            "RPC server for Client ", getClientRpcAddress()));
    getClientRpcServer().start();
  }

  public void stop() {
    try {
      LOG.info("Stopping the RPC server for Client Protocol");
      getClientRpcServer().stop();
    } catch (Exception ex) {
      LOG.error("Client Protocol RPC stop failed.", ex);
    }
    IOUtils.cleanupWithLogger(LOG, scm.getScmNodeManager());
  }

  public void join() throws InterruptedException {
    LOG.trace("Join RPC server for Client Protocol");
    getClientRpcServer().join();
  }

  @VisibleForTesting
  public String getRpcRemoteUsername() {
    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();
    return user == null ? null : user.getUserName();
  }

  @Override
  public ContainerWithPipeline allocateContainer(HddsProtos.ReplicationType
      replicationType, HddsProtos.ReplicationFactor factor,
      String owner) throws IOException {
    ScmUtils.preCheck(ScmOps.allocateContainer, safeModePrecheck);
    getScm().checkAdminAccess(getRpcRemoteUsername());

    final ContainerInfo container = scm.getContainerManager()
        .allocateContainer(replicationType, factor, owner);
    final Pipeline pipeline = scm.getPipelineManager()
        .getPipeline(container.getPipelineID());
    return new ContainerWithPipeline(container, pipeline);
  }

  @Override
  public ContainerInfo getContainer(long containerID) throws IOException {
    String remoteUser = getRpcRemoteUsername();
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    getScm().checkAdminAccess(remoteUser);
    try {
      return scm.getContainerManager()
          .getContainer(ContainerID.valueof(containerID));
    } catch (IOException ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.GET_CONTAINER, auditMap, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.GET_CONTAINER, auditMap)
        );
      }
    }

  }

  @Override
  public ContainerWithPipeline getContainerWithPipeline(long containerID)
      throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    boolean auditSuccess = true;
    try {
      if (safeModePrecheck.isInSafeMode()) {
        ContainerInfo contInfo = scm.getContainerManager()
            .getContainer(ContainerID.valueof(containerID));
        if (contInfo.isOpen()) {
          if (!hasRequiredReplicas(contInfo)) {
            throw new SCMException("Open container " + containerID + " doesn't"
                + " have enough replicas to service this operation in "
                + "Safe mode.", ResultCodes.SAFE_MODE_EXCEPTION);
          }
        }
      }
      getScm().checkAdminAccess(null);

      final ContainerID id = ContainerID.valueof(containerID);
      final ContainerInfo container = scm.getContainerManager().
          getContainer(id);
      final Pipeline pipeline;

      if (container.isOpen()) {
        // Ratis pipeline
        pipeline = scm.getPipelineManager()
            .getPipeline(container.getPipelineID());
      } else {
        pipeline = scm.getPipelineManager().createPipeline(
            HddsProtos.ReplicationType.STAND_ALONE,
            container.getReplicationFactor(),
            scm.getContainerManager()
                .getContainerReplicas(id).stream()
                .map(ContainerReplica::getDatanodeDetails)
                .collect(Collectors.toList()));
      }

      return new ContainerWithPipeline(container, pipeline);
    } catch (IOException ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.GET_CONTAINER_WITH_PIPELINE,
              auditMap, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.GET_CONTAINER_WITH_PIPELINE,
                auditMap)
        );
      }
    }
  }

  /**
   * Check if container reported replicas are equal or greater than required
   * replication factor.
   */
  private boolean hasRequiredReplicas(ContainerInfo contInfo) {
    try{
      return getScm().getContainerManager()
          .getContainerReplicas(contInfo.containerID())
          .size() >= contInfo.getReplicationFactor().getNumber();
    } catch (ContainerNotFoundException ex) {
      // getContainerReplicas throws exception if no replica's exist for given
      // container.
      return false;
    }
  }

  @Override
  public List<ContainerInfo> listContainer(long startContainerID,
      int count) throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("startContainerID", String.valueOf(startContainerID));
    auditMap.put("count", String.valueOf(count));
    try {
      // To allow startcontainerId to take the value "0",
      // "null" is assigned, so that its handled in the
      // scm.getContainerManager().listContainer method
      final ContainerID containerId = startContainerID != 0 ? ContainerID
          .valueof(startContainerID) : null;
      return scm.getContainerManager().
          listContainer(containerId, count);
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.LIST_CONTAINER, auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.LIST_CONTAINER, auditMap));
      }
    }

  }

  @Override
  public void deleteContainer(long containerID) throws IOException {
    String remoteUser = getRpcRemoteUsername();
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    auditMap.put("remoteUser", remoteUser);
    try {
      getScm().checkAdminAccess(remoteUser);
      scm.getContainerManager().deleteContainer(
          ContainerID.valueof(containerID));
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(SCMAction.DELETE_CONTAINER, auditMap, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(SCMAction.DELETE_CONTAINER, auditMap)
        );
      }
    }
  }

  @Override
  public List<HddsProtos.Node> queryNode(HddsProtos.NodeState state,
      HddsProtos.QueryScope queryScope, String poolName) throws
      IOException {

    if (queryScope == HddsProtos.QueryScope.POOL) {
      throw new IllegalArgumentException("Not Supported yet");
    }

    List<HddsProtos.Node> result = new ArrayList<>();
    queryNode(state).forEach(node -> result.add(HddsProtos.Node.newBuilder()
        .setNodeID(node.getProtoBufMessage())
        .addNodeStates(state)
        .build()));

    return result;

  }

  @Override
  public void notifyObjectStageChange(StorageContainerLocationProtocolProtos
      .ObjectStageChangeRequestProto.Type type, long id,
      StorageContainerLocationProtocolProtos.ObjectStageChangeRequestProto.Op
          op, StorageContainerLocationProtocolProtos
      .ObjectStageChangeRequestProto.Stage stage) throws IOException {

    LOG.info("Object type {} id {} op {} new stage {}", type, id, op,
        stage);
    if (type == StorageContainerLocationProtocolProtos
        .ObjectStageChangeRequestProto.Type.container) {
      if (op == StorageContainerLocationProtocolProtos
          .ObjectStageChangeRequestProto.Op.close) {
        if (stage == StorageContainerLocationProtocolProtos
            .ObjectStageChangeRequestProto.Stage.begin) {
          scm.getContainerManager()
              .updateContainerState(ContainerID.valueof(id),
                  HddsProtos.LifeCycleEvent.FINALIZE);
        } else {
          scm.getContainerManager()
              .updateContainerState(ContainerID.valueof(id),
                  HddsProtos.LifeCycleEvent.CLOSE);
        }
      }
    } // else if (type == ObjectStageChangeRequestProto.Type.pipeline) {
    // TODO: pipeline state update will be addressed in future patch.
    // }

  }

  @Override
  public Pipeline createReplicationPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, HddsProtos.NodePool nodePool)
      throws IOException {
    // TODO: will be addressed in future patch.
    // This is needed only for debugging purposes to make sure cluster is
    // working correctly.
    return null;
  }

  @Override
  public List<Pipeline> listPipelines() {
    AUDIT.logReadSuccess(
        buildAuditMessageForSuccess(SCMAction.LIST_PIPELINE, null));
    return scm.getPipelineManager().getPipelines();
  }

  @Override
  public void closePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("pipelineID", pipelineID.getId());
    PipelineManager pipelineManager = scm.getPipelineManager();
    Pipeline pipeline =
        pipelineManager.getPipeline(PipelineID.getFromProtobuf(pipelineID));
    pipelineManager.finalizeAndDestroyPipeline(pipeline, false);
    AUDIT.logWriteSuccess(
        buildAuditMessageForSuccess(SCMAction.CLOSE_PIPELINE, null)
    );
  }

  @Override
  public ScmInfo getScmInfo() throws IOException {
    boolean auditSuccess = true;
    try{
      ScmInfo.Builder builder =
          new ScmInfo.Builder()
              .setClusterId(scm.getScmStorageConfig().getClusterID())
              .setScmId(scm.getScmStorageConfig().getScmId());
      return builder.build();
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.GET_SCM_INFO, null, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.GET_SCM_INFO, null)
        );
      }
    }
  }

  /**
   * Check if SCM is in safe mode.
   *
   * @return Returns true if SCM is in safe mode else returns false.
   * @throws IOException
   */
  @Override
  public boolean inSafeMode() throws IOException {
    AUDIT.logReadSuccess(
        buildAuditMessageForSuccess(SCMAction.IN_SAFE_MODE, null)
    );
    return scm.isInSafeMode();
  }

  /**
   * Force SCM out of Safe mode.
   *
   * @return returns true if operation is successful.
   * @throws IOException
   */
  @Override
  public boolean forceExitSafeMode() throws IOException {
    AUDIT.logWriteSuccess(
        buildAuditMessageForSuccess(SCMAction.FORCE_EXIT_SAFE_MODE, null)
    );
    return scm.exitSafeMode();
  }

  /**
   * Queries a list of Node that match a set of statuses.
   *
   * <p>For example, if the nodeStatuses is HEALTHY and RAFT_MEMBER, then
   * this call will return all
   * healthy nodes which members in Raft pipeline.
   *
   * <p>Right now we don't support operations, so we assume it is an AND
   * operation between the
   * operators.
   *
   * @param state - NodeStates.
   * @return List of Datanodes.
   */
  public List<DatanodeDetails> queryNode(HddsProtos.NodeState state) {
    Preconditions.checkNotNull(state, "Node Query set cannot be null");
    return new ArrayList<>(queryNodeState(state));
  }

  @VisibleForTesting
  public StorageContainerManager getScm() {
    return scm;
  }

  /**
   * Set safe mode status based on .
   */
  public boolean getSafeModeStatus() {
    return safeModePrecheck.isInSafeMode();
  }


  /**
   * Query the System for Nodes.
   *
   * @param nodeState - NodeState that we are interested in matching.
   * @return Set of Datanodes that match the NodeState.
   */
  private Set<DatanodeDetails> queryNodeState(HddsProtos.NodeState nodeState) {
    Set<DatanodeDetails> returnSet = new TreeSet<>();
    List<DatanodeDetails> tmp = scm.getScmNodeManager().getNodes(nodeState);
    if ((tmp != null) && (tmp.size() > 0)) {
      returnSet.addAll(tmp);
    }
    return returnSet;
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

  @Override
  public void close() throws IOException {
    stop();
  }

  /**
   * Set SafeMode status.
   *
   * @param safeModeStatus
   */
  public void setSafeModeStatus(boolean safeModeStatus) {
    safeModePrecheck.setInSafeMode(safeModeStatus);
  }
}
