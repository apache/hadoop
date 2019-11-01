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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.GetScmInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ActivatePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ClosePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DeactivatePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.InSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.NodeQueryRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.NodeQueryResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ObjectStageChangeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.PipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.PipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerStatusRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerStatusResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMDeleteContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationRequest.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationResponse;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartReplicationManagerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StopReplicationManagerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

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
   * Helper method to wrap the request and send the message.
   */
  private ScmContainerLocationResponse submitRequest(
      StorageContainerLocationProtocolProtos.Type type,
      Consumer<Builder> builderConsumer) throws IOException {
    final ScmContainerLocationResponse response;
    try {

      Builder builder = ScmContainerLocationRequest.newBuilder()
          .setCmdType(type)
          .setTraceID(TracingUtil.exportCurrentSpan());
      builderConsumer.accept(builder);
      ScmContainerLocationRequest wrapper = builder.build();

      response = rpcProxy.submitRequest(NULL_RPC_CONTROLLER, wrapper);
    } catch (ServiceException ex) {
      throw ProtobufHelper.getRemoteException(ex);
    }
    return response;
  }

  /**
   * Asks SCM where a container should be allocated. SCM responds with the set
   * of datanodes that should be used creating this container. Ozone/SCM only
   * supports replication factor of either 1 or 3.
   *
   * @param type   - Replication Type
   * @param factor - Replication Count
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

    ContainerResponseProto response =
        submitRequest(Type.AllocateContainer,
            builder -> builder.setContainerRequest(request))
            .getContainerResponse();
    //TODO should be migrated to use the top level status structure.
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
    ScmContainerLocationResponse response =
        submitRequest(Type.GetContainer,
            (builder) -> builder.setGetContainerRequest(request));
    return ContainerInfo
        .fromProtobuf(response.getGetContainerResponse().getContainerInfo());

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

    ScmContainerLocationResponse response =
        submitRequest(Type.GetContainerWithPipeline,
            (builder) -> builder.setGetContainerWithPipelineRequest(request));

    return ContainerWithPipeline.fromProtobuf(
        response.getGetContainerWithPipelineResponse()
            .getContainerWithPipeline());

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

    SCMListContainerResponseProto response =
        submitRequest(Type.ListContainer,
            builder1 -> builder1.setScmListContainerRequest(request))
            .getScmListContainerResponse();
    List<ContainerInfo> containerList = new ArrayList<>();
    for (HddsProtos.ContainerInfoProto containerInfoProto : response
        .getContainersList()) {
      containerList.add(ContainerInfo.fromProtobuf(containerInfoProto));
    }
    return containerList;

  }

  /**
   * Ask SCM to delete a container by name. SCM will remove
   * the container mapping in its database.
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
    submitRequest(Type.DeleteContainer,
        builder -> builder.setScmDeleteContainerRequest(request));

  }

  /**
   * Queries a list of Node Statuses.
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
    NodeQueryResponseProto response = submitRequest(Type.QueryNode,
        builder -> builder.setNodeQueryRequest(request)).getNodeQueryResponse();
    return response.getDatanodesList();

  }

  /**
   * Notify from client that creates object on datanodes.
   *
   * @param type  object type
   * @param id    object id
   * @param op    operation type (e.g., create, close, delete)
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
    submitRequest(Type.NotifyObjectStageChange,
        builder -> builder.setObjectStageChangeRequest(request));

  }

  /**
   * Creates a replication pipeline of a specified type.
   *
   * @param replicationType - replication type
   * @param factor          - factor 1 or 3
   * @param nodePool        - optional machine list to build a pipeline.
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

    PipelineResponseProto response =
        submitRequest(Type.AllocatePipeline,
            builder -> builder.setPipelineRequest(request))
            .getPipelineResponse();
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

  }

  @Override
  public List<Pipeline> listPipelines() throws IOException {
    ListPipelineRequestProto request = ListPipelineRequestProto
        .newBuilder().setTraceID(TracingUtil.exportCurrentSpan())
        .build();

    ListPipelineResponseProto response = submitRequest(Type.ListPipelines,
        builder -> builder.setListPipelineRequest(request))
        .getListPipelineResponse();

    List<Pipeline> list = new ArrayList<>();
    for (HddsProtos.Pipeline pipeline : response.getPipelinesList()) {
      Pipeline fromProtobuf = Pipeline.getFromProtobuf(pipeline);
      list.add(fromProtobuf);
    }
    return list;

  }

  @Override
  public void activatePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    ActivatePipelineRequestProto request =
        ActivatePipelineRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .setPipelineID(pipelineID)
            .build();
    submitRequest(Type.ActivatePipeline,
        builder -> builder.setActivatePipelineRequest(request));

  }

  @Override
  public void deactivatePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {

    DeactivatePipelineRequestProto request =
        DeactivatePipelineRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .setPipelineID(pipelineID)
            .build();
    submitRequest(Type.DeactivatePipeline,
        builder -> builder.setDeactivatePipelineRequest(request));
  }

  @Override
  public void closePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {

    ClosePipelineRequestProto request =
        ClosePipelineRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .setPipelineID(pipelineID)
            .build();
    submitRequest(Type.ClosePipeline,
        builder -> builder.setClosePipelineRequest(request));

  }

  @Override
  public ScmInfo getScmInfo() throws IOException {
    HddsProtos.GetScmInfoRequestProto request =
        HddsProtos.GetScmInfoRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .build();

    GetScmInfoResponseProto resp = submitRequest(Type.GetScmInfo,
        builder -> builder.setGetScmInfoRequest(request))
        .getGetScmInfoResponse();
    ScmInfo.Builder builder = new ScmInfo.Builder()
        .setClusterId(resp.getClusterId())
        .setScmId(resp.getScmId());
    return builder.build();

  }

  /**
   * Check if SCM is in safe mode.
   *
   * @return Returns true if SCM is in safe mode else returns false.
   */
  @Override
  public boolean inSafeMode() throws IOException {
    InSafeModeRequestProto request =
        InSafeModeRequestProto.getDefaultInstance();

    return submitRequest(Type.InSafeMode,
        builder -> builder.setInSafeModeRequest(request))
        .getInSafeModeResponse().getInSafeMode();

  }

  /**
   * Force SCM out of Safe mode.
   *
   * @return returns true if operation is successful.
   */
  @Override
  public boolean forceExitSafeMode() throws IOException {
    ForceExitSafeModeRequestProto request =
        ForceExitSafeModeRequestProto.getDefaultInstance();
    ForceExitSafeModeResponseProto resp =
        submitRequest(Type.ForceExitSafeMode,
            builder -> builder.setForceExitSafeModeRequest(request))
            .getForceExitSafeModeResponse();

    return resp.getExitedSafeMode();

  }

  @Override
  public void startReplicationManager() throws IOException {

    StartReplicationManagerRequestProto request =
        StartReplicationManagerRequestProto.getDefaultInstance();
    submitRequest(Type.StartReplicationManager,
        builder -> builder.setStartReplicationManagerRequest(request));

  }

  @Override
  public void stopReplicationManager() throws IOException {

    StopReplicationManagerRequestProto request =
        StopReplicationManagerRequestProto.getDefaultInstance();
    submitRequest(Type.StopReplicationManager,
        builder -> builder.setStopReplicationManagerRequest(request));

  }

  @Override
  public boolean getReplicationManagerStatus() throws IOException {

    ReplicationManagerStatusRequestProto request =
        ReplicationManagerStatusRequestProto.getDefaultInstance();
    ReplicationManagerStatusResponseProto response =
        submitRequest(Type.GetReplicationManagerStatus,
            builder -> builder.setSeplicationManagerStatusRequest(request))
            .getReplicationManagerStatusResponse();
    return response.getIsRunning();

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
