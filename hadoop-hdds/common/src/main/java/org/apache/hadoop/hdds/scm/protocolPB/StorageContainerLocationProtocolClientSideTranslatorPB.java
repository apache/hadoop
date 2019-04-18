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
package org.apache.hadoop.hdds.scm.protocolPB;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ClosePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.InSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.InSafeModeResponseProto;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.ContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.ContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.GetContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.GetContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.NodeQueryRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.NodeQueryResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.ObjectStageChangeRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.PipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.PipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.SCMDeleteContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.SCMListContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.SCMListContainerResponseProto;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is the client-side translator to translate the requests made on
 * the {@link StorageContainerLocationProtocol} interface to the RPC server
 * implementing {@link StorageContainerLocationProtocolPB}.
 */
@InterfaceAudience.Private
public final class StorageContainerLocationProtocolClientSideTranslatorPB
    implements StorageContainerLocationProtocol, ProtocolTranslator, Closeable {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final StorageContainerLocationProtocolPB rpcProxy;

  /**
   * Creates a new StorageContainerLocationProtocolClientSideTranslatorPB.
   *
   * @param rpcProxy {@link StorageContainerLocationProtocolPB} RPC proxy
   */
  public StorageContainerLocationProtocolClientSideTranslatorPB(
      StorageContainerLocationProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  /**
   * Asks SCM where a container should be allocated. SCM responds with the set
   * of datanodes that should be used creating this container. Ozone/SCM only
   * supports replication factor of either 1 or 3.
   * @param type - Replication Type
   * @param factor - Replication Count
   * @return
   * @throws IOException
   */
  @Override
  public ContainerWithPipeline allocateContainer(
      HddsProtos.ReplicationType type, HddsProtos.ReplicationFactor factor,
      String owner) throws IOException {

    ContainerRequestProto request = ContainerRequestProto.newBuilder()
        .setTraceID(TracingUtil.exportCurrentSpan())
        .setReplicationFactor(factor)
        .setReplicationType(type)
        .setOwner(owner)
        .build();

    final ContainerResponseProto response;
    try {
      response = rpcProxy.allocateContainer(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    if (response.getErrorCode() != ContainerResponseProto.Error.success) {
      throw new IOException(response.hasErrorMessage() ?
          response.getErrorMessage() : "Allocate container failed.");
    }
    return ContainerWithPipeline.fromProtobuf(
        response.getContainerWithPipeline());
  }

  public ContainerInfo getContainer(long containerID) throws IOException {
    Preconditions.checkState(containerID >= 0,
        "Container ID cannot be negative");
    GetContainerRequestProto request = GetContainerRequestProto
        .newBuilder()
        .setContainerID(containerID)
        .setTraceID(TracingUtil.exportCurrentSpan())
        .build();
    try {
      GetContainerResponseProto response =
          rpcProxy.getContainer(NULL_RPC_CONTROLLER, request);
      return ContainerInfo.fromProtobuf(response.getContainerInfo());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  public ContainerWithPipeline getContainerWithPipeline(long containerID)
      throws IOException {
    Preconditions.checkState(containerID >= 0,
        "Container ID cannot be negative");
    GetContainerWithPipelineRequestProto request =
        GetContainerWithPipelineRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .setContainerID(containerID).build();
    try {
      GetContainerWithPipelineResponseProto response =
          rpcProxy.getContainerWithPipeline(NULL_RPC_CONTROLLER, request);
      return ContainerWithPipeline.fromProtobuf(
          response.getContainerWithPipeline());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ContainerInfo> listContainer(long startContainerID, int count)
      throws IOException {
    Preconditions.checkState(startContainerID >= 0,
        "Container ID cannot be negative.");
    Preconditions.checkState(count > 0,
        "Container count must be greater than 0.");
    SCMListContainerRequestProto.Builder builder = SCMListContainerRequestProto
        .newBuilder();
    builder.setStartContainerID(startContainerID);
    builder.setCount(count);
    builder.setTraceID(TracingUtil.exportCurrentSpan());
    SCMListContainerRequestProto request = builder.build();

    try {
      SCMListContainerResponseProto response =
          rpcProxy.listContainer(NULL_RPC_CONTROLLER, request);
      List<ContainerInfo> containerList = new ArrayList<>();
      for (HddsProtos.ContainerInfoProto containerInfoProto : response
          .getContainersList()) {
        containerList.add(ContainerInfo.fromProtobuf(containerInfoProto));
      }
      return containerList;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Ask SCM to delete a container by name. SCM will remove
   * the container mapping in its database.
   *
   * @param containerID
   * @throws IOException
   */
  @Override
  public void deleteContainer(long containerID)
      throws IOException {
    Preconditions.checkState(containerID >= 0,
        "Container ID cannot be negative");
    SCMDeleteContainerRequestProto request = SCMDeleteContainerRequestProto
        .newBuilder()
        .setTraceID(TracingUtil.exportCurrentSpan())
        .setContainerID(containerID)
        .build();
    try {
      rpcProxy.deleteContainer(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Queries a list of Node Statuses.
   *
   * @param nodeStatuses
   * @return List of Datanodes.
   */
  @Override
  public List<HddsProtos.Node> queryNode(HddsProtos.NodeState
      nodeStatuses, HddsProtos.QueryScope queryScope, String poolName)
      throws IOException {
    // TODO : We support only cluster wide query right now. So ignoring checking
    // queryScope and poolName
    Preconditions.checkNotNull(nodeStatuses);
    NodeQueryRequestProto request = NodeQueryRequestProto.newBuilder()
        .setState(nodeStatuses)
        .setTraceID(TracingUtil.exportCurrentSpan())
        .setScope(queryScope).setPoolName(poolName).build();
    try {
      NodeQueryResponseProto response =
          rpcProxy.queryNode(NULL_RPC_CONTROLLER, request);
      return response.getDatanodesList();
    } catch (ServiceException e) {
      throw  ProtobufHelper.getRemoteException(e);
    }

  }

  /**
   * Notify from client that creates object on datanodes.
   * @param type object type
   * @param id object id
   * @param op operation type (e.g., create, close, delete)
   * @param stage object creation stage : begin/complete
   */
  @Override
  public void notifyObjectStageChange(
      ObjectStageChangeRequestProto.Type type, long id,
      ObjectStageChangeRequestProto.Op op,
      ObjectStageChangeRequestProto.Stage stage) throws IOException {
    Preconditions.checkState(id >= 0,
        "Object id cannot be negative.");
    ObjectStageChangeRequestProto request =
        ObjectStageChangeRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .setType(type)
            .setId(id)
            .setOp(op)
            .setStage(stage)
            .build();
    try {
      rpcProxy.notifyObjectStageChange(NULL_RPC_CONTROLLER, request);
    } catch(ServiceException e){
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Creates a replication pipeline of a specified type.
   *
   * @param replicationType - replication type
   * @param factor - factor 1 or 3
   * @param nodePool - optional machine list to build a pipeline.
   * @throws IOException
   */
  @Override
  public Pipeline createReplicationPipeline(HddsProtos.ReplicationType
      replicationType, HddsProtos.ReplicationFactor factor, HddsProtos
      .NodePool nodePool) throws IOException {
    PipelineRequestProto request = PipelineRequestProto.newBuilder()
        .setTraceID(TracingUtil.exportCurrentSpan())
        .setNodePool(nodePool)
        .setReplicationFactor(factor)
        .setReplicationType(replicationType)
        .build();
    try {
      PipelineResponseProto response =
          rpcProxy.allocatePipeline(NULL_RPC_CONTROLLER, request);
      if (response.getErrorCode() ==
          PipelineResponseProto.Error.success) {
        Preconditions.checkState(response.hasPipeline(), "With success, " +
            "must come a pipeline");
        return Pipeline.getFromProtobuf(response.getPipeline());
      } else {
        String errorMessage = String.format("create replication pipeline " +
                "failed. code : %s Message: %s", response.getErrorCode(),
            response.hasErrorMessage() ? response.getErrorMessage() : "");
        throw new IOException(errorMessage);
      }
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public List<Pipeline> listPipelines() throws IOException {
    try {
      ListPipelineRequestProto request = ListPipelineRequestProto
          .newBuilder().setTraceID(TracingUtil.exportCurrentSpan())
          .build();
      ListPipelineResponseProto response = rpcProxy.listPipelines(
          NULL_RPC_CONTROLLER, request);
      List<Pipeline> list = new ArrayList<>();
      for (HddsProtos.Pipeline pipeline : response.getPipelinesList()) {
        Pipeline fromProtobuf = Pipeline.getFromProtobuf(pipeline);
        list.add(fromProtobuf);
      }
      return list;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void closePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    try {
      ClosePipelineRequestProto request =
          ClosePipelineRequestProto.newBuilder()
              .setTraceID(TracingUtil.exportCurrentSpan())
              .setPipelineID(pipelineID)
          .build();
      rpcProxy.closePipeline(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public ScmInfo getScmInfo() throws IOException {
    HddsProtos.GetScmInfoRequestProto request =
        HddsProtos.GetScmInfoRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .build();
    try {
      HddsProtos.GetScmInfoResponseProto resp = rpcProxy.getScmInfo(
          NULL_RPC_CONTROLLER, request);
      ScmInfo.Builder builder = new ScmInfo.Builder()
          .setClusterId(resp.getClusterId())
          .setScmId(resp.getScmId());
      return builder.build();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
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
    InSafeModeRequestProto request =
        InSafeModeRequestProto.getDefaultInstance();
    try {
      InSafeModeResponseProto resp = rpcProxy.inSafeMode(
          NULL_RPC_CONTROLLER, request);
      return resp.getInSafeMode();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Force SCM out of Safe mode.
   *
   * @return returns true if operation is successful.
   * @throws IOException
   */
  @Override
  public boolean forceExitSafeMode() throws IOException {
    ForceExitSafeModeRequestProto request =
        ForceExitSafeModeRequestProto.getDefaultInstance();
    try {
      ForceExitSafeModeResponseProto resp = rpcProxy
          .forceExitSafeMode(NULL_RPC_CONTROLLER, request);
      return resp.getExitedSafeMode();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }
}
