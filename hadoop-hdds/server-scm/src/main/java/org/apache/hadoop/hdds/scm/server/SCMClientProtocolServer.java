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
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.protocolPB
    .StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
    StorageContainerLocationProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMClientProtocolServer.class);
  private final RPC.Server clientRpcServer;
  private final InetSocketAddress clientRpcAddress;
  private final StorageContainerManager scm;
  private final OzoneConfiguration conf;

  public SCMClientProtocolServer(OzoneConfiguration conf,
      StorageContainerManager scm) throws IOException {
    this.scm = scm;
    this.conf = conf;
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
  public ContainerInfo allocateContainer(HddsProtos.ReplicationType
      replicationType, HddsProtos.ReplicationFactor factor,
      String owner) throws IOException {
    String remoteUser = getRpcRemoteUsername();
    getScm().checkAdminAccess(remoteUser);
    return scm.getScmContainerManager()
        .allocateContainer(replicationType, factor, owner);
  }

  @Override
  public ContainerInfo getContainer(long containerID) throws IOException {
    String remoteUser = getRpcRemoteUsername();
    getScm().checkAdminAccess(remoteUser);
    return scm.getScmContainerManager()
        .getContainer(containerID);
  }

  @Override
  public List<ContainerInfo> listContainer(long startContainerID,
      int count) throws IOException {
    return scm.getScmContainerManager().
        listContainer(startContainerID, count);
  }

  @Override
  public void deleteContainer(long containerID) throws IOException {
    String remoteUser = getRpcRemoteUsername();
    getScm().checkAdminAccess(remoteUser);
    scm.getScmContainerManager().deleteContainer(containerID);

  }

  @Override
  public HddsProtos.NodePool queryNode(EnumSet<HddsProtos.NodeState>
      nodeStatuses, HddsProtos.QueryScope queryScope, String poolName) throws
      IOException {

    if (queryScope == HddsProtos.QueryScope.POOL) {
      throw new IllegalArgumentException("Not Supported yet");
    }

    List<DatanodeDetails> datanodes = queryNode(nodeStatuses);
    HddsProtos.NodePool.Builder poolBuilder = HddsProtos.NodePool.newBuilder();

    for (DatanodeDetails datanode : datanodes) {
      HddsProtos.Node node =
          HddsProtos.Node.newBuilder()
              .setNodeID(datanode.getProtoBufMessage())
              .addAllNodeStates(nodeStatuses)
              .build();
      poolBuilder.addNodes(node);
    }

    return poolBuilder.build();

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
          .ObjectStageChangeRequestProto.Op.create) {
        if (stage == StorageContainerLocationProtocolProtos
            .ObjectStageChangeRequestProto.Stage.begin) {
          scm.getScmContainerManager().updateContainerState(id, HddsProtos
              .LifeCycleEvent.CREATE);
        } else {
          scm.getScmContainerManager().updateContainerState(id, HddsProtos
              .LifeCycleEvent.CREATED);
        }
      } else {
        if (op == StorageContainerLocationProtocolProtos
            .ObjectStageChangeRequestProto.Op.close) {
          if (stage == StorageContainerLocationProtocolProtos
              .ObjectStageChangeRequestProto.Stage.begin) {
            scm.getScmContainerManager().updateContainerState(id, HddsProtos
                .LifeCycleEvent.FINALIZE);
          } else {
            scm.getScmContainerManager().updateContainerState(id, HddsProtos
                .LifeCycleEvent.CLOSE);
          }
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
  public ScmInfo getScmInfo() throws IOException {
    ScmInfo.Builder builder =
        new ScmInfo.Builder()
            .setClusterId(scm.getScmStorage().getClusterID())
            .setScmId(scm.getScmStorage().getScmId());
    return builder.build();
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
   * @param nodeStatuses - A set of NodeStates.
   * @return List of Datanodes.
   */
  public List<DatanodeDetails> queryNode(EnumSet<HddsProtos.NodeState>
      nodeStatuses) {
    Preconditions.checkNotNull(nodeStatuses, "Node Query set cannot be null");
    Preconditions.checkState(nodeStatuses.size() > 0, "No valid arguments " +
        "in the query set");
    List<DatanodeDetails> resultList = new LinkedList<>();
    Set<DatanodeDetails> currentSet = new TreeSet<>();

    for (HddsProtos.NodeState nodeState : nodeStatuses) {
      Set<DatanodeDetails> nextSet = queryNodeState(nodeState);
      if ((nextSet == null) || (nextSet.size() == 0)) {
        // Right now we only support AND operation. So intersect with
        // any empty set is null.
        return resultList;
      }
      // First time we have to add all the elements, next time we have to
      // do an intersection operation on the set.
      if (currentSet.size() == 0) {
        currentSet.addAll(nextSet);
      } else {
        currentSet.retainAll(nextSet);
      }
    }

    resultList.addAll(currentSet);
    return resultList;
  }

  @VisibleForTesting
  public StorageContainerManager getScm() {
    return scm;
  }

  /**
   * Query the System for Nodes.
   *
   * @param nodeState - NodeState that we are interested in matching.
   * @return Set of Datanodes that match the NodeState.
   */
  private Set<DatanodeDetails> queryNodeState(HddsProtos.NodeState nodeState) {
    if (nodeState == HddsProtos.NodeState.RAFT_MEMBER || nodeState ==
        HddsProtos.NodeState
        .FREE_NODE) {
      throw new IllegalStateException("Not implemented yet");
    }
    Set<DatanodeDetails> returnSet = new TreeSet<>();
    List<DatanodeDetails> tmp = scm.getScmNodeManager().getNodes(nodeState);
    if ((tmp != null) && (tmp.size() > 0)) {
      returnSet.addAll(tmp);
    }
    return returnSet;
  }
}
