/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.protocol;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ActivatePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ActivatePipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ClosePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ClosePipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DeactivatePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DeactivatePipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.InSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.InSafeModeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.NodeQueryResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ObjectStageChangeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ObjectStageChangeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerStatusRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerStatusResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMDeleteContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMDeleteContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationResponse;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationResponse.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartReplicationManagerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartReplicationManagerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StopReplicationManagerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StopReplicationManagerResponseProto;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.ozone.protocolPB.ProtocolMessageMetrics;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link StorageContainerLocationProtocolPB} to the
 * {@link StorageContainerLocationProtocol} server implementation.
 */
@InterfaceAudience.Private
public final class StorageContainerLocationProtocolServerSideTranslatorPB
    implements StorageContainerLocationProtocolPB {

  private static final Logger LOG =
      LoggerFactory.getLogger(
          StorageContainerLocationProtocolServerSideTranslatorPB.class);

  private final StorageContainerLocationProtocol impl;

  private OzoneProtocolMessageDispatcher<ScmContainerLocationRequest,
      ScmContainerLocationResponse>
      dispatcher;

  /**
   * Creates a new StorageContainerLocationProtocolServerSideTranslatorPB.
   *
   * @param impl            {@link StorageContainerLocationProtocol} server
   *                        implementation
   * @param protocolMetrics
   */
  public StorageContainerLocationProtocolServerSideTranslatorPB(
      StorageContainerLocationProtocol impl,
      ProtocolMessageMetrics protocolMetrics) throws IOException {
    this.impl = impl;
    this.dispatcher =
        new OzoneProtocolMessageDispatcher<>("ScmContainerLocation",
            protocolMetrics, LOG);
  }

  @Override
  public ScmContainerLocationResponse submitRequest(RpcController controller,
      ScmContainerLocationRequest request) throws ServiceException {
    return dispatcher
        .processRequest(request, this::processRequest, request.getCmdType(),
            request.getTraceID());
  }

  public ScmContainerLocationResponse processRequest(
      ScmContainerLocationRequest request) throws ServiceException {
    try {
      switch (request.getCmdType()) {
      case AllocateContainer:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setContainerResponse(
                allocateContainer(request.getContainerRequest()))
            .build();
      case GetContainer:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetContainerResponse(
                getContainer(request.getGetContainerRequest()))
            .build();
      case GetContainerWithPipeline:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetContainerWithPipelineResponse(getContainerWithPipeline(
                request.getGetContainerWithPipelineRequest()))
            .build();
      case ListContainer:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setScmListContainerResponse(listContainer(
                request.getScmListContainerRequest()))
            .build();
      case QueryNode:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setNodeQueryResponse(queryNode(request.getNodeQueryRequest()))
            .build();
      case NotifyObjectStageChange:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setObjectStageChangeResponse(notifyObjectStageChange(
                request.getObjectStageChangeRequest()))
            .build();
      case ListPipelines:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setListPipelineResponse(listPipelines(
                request.getListPipelineRequest()))
            .build();
      case ActivatePipeline:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setActivatePipelineResponse(activatePipeline(
                request.getActivatePipelineRequest()))
            .build();
      case GetScmInfo:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetScmInfoResponse(getScmInfo(
                request.getGetScmInfoRequest()))
            .build();
      case InSafeMode:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setInSafeModeResponse(inSafeMode(
                request.getInSafeModeRequest()))
            .build();
      case ForceExitSafeMode:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setForceExitSafeModeResponse(forceExitSafeMode(
                request.getForceExitSafeModeRequest()))
            .build();
      case StartReplicationManager:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setStartReplicationManagerResponse(startReplicationManager(
                request.getStartReplicationManagerRequest()))
            .build();
      case StopReplicationManager:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setStopReplicationManagerResponse(stopReplicationManager(
                request.getStopReplicationManagerRequest()))
            .build();
      case GetReplicationManagerStatus:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setReplicationManagerStatusResponse(getReplicationManagerStatus(
                request.getSeplicationManagerStatusRequest()))
            .build();
      default:
        throw new IllegalArgumentException(
            "Unknown command type: " + request.getCmdType());
      }

    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  public ContainerResponseProto allocateContainer(ContainerRequestProto request)
      throws IOException {
    ContainerWithPipeline containerWithPipeline = impl
        .allocateContainer(request.getReplicationType(),
            request.getReplicationFactor(), request.getOwner());
    return ContainerResponseProto.newBuilder()
        .setContainerWithPipeline(containerWithPipeline.getProtobuf())
        .setErrorCode(ContainerResponseProto.Error.success)
        .build();

  }

  public GetContainerResponseProto getContainer(
      GetContainerRequestProto request) throws IOException {
    ContainerInfo container = impl.getContainer(request.getContainerID());
    return GetContainerResponseProto.newBuilder()
        .setContainerInfo(container.getProtobuf())
        .build();
  }

  public GetContainerWithPipelineResponseProto getContainerWithPipeline(
      GetContainerWithPipelineRequestProto request)
      throws IOException {
    ContainerWithPipeline container = impl
        .getContainerWithPipeline(request.getContainerID());
    return GetContainerWithPipelineResponseProto.newBuilder()
        .setContainerWithPipeline(container.getProtobuf())
        .build();
  }

  public SCMListContainerResponseProto listContainer(
      SCMListContainerRequestProto request) throws IOException {

    long startContainerID = 0;
    int count = -1;

    // Arguments check.
    if (request.hasStartContainerID()) {
      // End container name is given.
      startContainerID = request.getStartContainerID();
    }
    count = request.getCount();
    List<ContainerInfo> containerList =
        impl.listContainer(startContainerID, count);
    SCMListContainerResponseProto.Builder builder =
        SCMListContainerResponseProto.newBuilder();
    for (ContainerInfo container : containerList) {
      builder.addContainers(container.getProtobuf());
    }
    return builder.build();
  }

  public SCMDeleteContainerResponseProto deleteContainer(
      SCMDeleteContainerRequestProto request)
      throws IOException {
    impl.deleteContainer(request.getContainerID());
    return SCMDeleteContainerResponseProto.newBuilder().build();

  }

  public NodeQueryResponseProto queryNode(
      StorageContainerLocationProtocolProtos.NodeQueryRequestProto request)
      throws IOException {

    HddsProtos.NodeState nodeState = request.getState();
    List<HddsProtos.Node> datanodes = impl.queryNode(nodeState,
        request.getScope(), request.getPoolName());
    return NodeQueryResponseProto.newBuilder()
        .addAllDatanodes(datanodes)
        .build();

  }

  public ObjectStageChangeResponseProto notifyObjectStageChange(
      ObjectStageChangeRequestProto request)
      throws IOException {
    impl.notifyObjectStageChange(request.getType(), request.getId(),
        request.getOp(), request.getStage());
    return ObjectStageChangeResponseProto.newBuilder().build();
  }

  public ListPipelineResponseProto listPipelines(
      ListPipelineRequestProto request)
      throws IOException {
    ListPipelineResponseProto.Builder builder = ListPipelineResponseProto
        .newBuilder();
    List<Pipeline> pipelines = impl.listPipelines();
    for (Pipeline pipeline : pipelines) {
      HddsProtos.Pipeline protobufMessage = pipeline.getProtobufMessage();
      builder.addPipelines(protobufMessage);
    }
    return builder.build();
  }

  public ActivatePipelineResponseProto activatePipeline(
      ActivatePipelineRequestProto request)
      throws IOException {
    impl.activatePipeline(request.getPipelineID());
    return ActivatePipelineResponseProto.newBuilder().build();
  }

  public DeactivatePipelineResponseProto deactivatePipeline(
      DeactivatePipelineRequestProto request)
      throws IOException {
    impl.deactivatePipeline(request.getPipelineID());
    return DeactivatePipelineResponseProto.newBuilder().build();
  }

  public ClosePipelineResponseProto closePipeline(
      RpcController controller, ClosePipelineRequestProto request)
      throws IOException {

    impl.closePipeline(request.getPipelineID());
    return ClosePipelineResponseProto.newBuilder().build();

  }

  public HddsProtos.GetScmInfoResponseProto getScmInfo(
      HddsProtos.GetScmInfoRequestProto req)
      throws IOException {
    ScmInfo scmInfo = impl.getScmInfo();
    return HddsProtos.GetScmInfoResponseProto.newBuilder()
        .setClusterId(scmInfo.getClusterId())
        .setScmId(scmInfo.getScmId())
        .build();

  }

  public InSafeModeResponseProto inSafeMode(
      InSafeModeRequestProto request) throws IOException {

    return InSafeModeResponseProto.newBuilder()
        .setInSafeMode(impl.inSafeMode()).build();

  }

  public ForceExitSafeModeResponseProto forceExitSafeMode(
      ForceExitSafeModeRequestProto request)
      throws IOException {
    return ForceExitSafeModeResponseProto.newBuilder()
        .setExitedSafeMode(impl.forceExitSafeMode()).build();

  }

  public StartReplicationManagerResponseProto startReplicationManager(
      StartReplicationManagerRequestProto request)
      throws IOException {
    impl.startReplicationManager();
    return StartReplicationManagerResponseProto.newBuilder().build();
  }

  public StopReplicationManagerResponseProto stopReplicationManager(
      StopReplicationManagerRequestProto request)
      throws IOException {
    impl.stopReplicationManager();
    return StopReplicationManagerResponseProto.newBuilder().build();

  }

  public ReplicationManagerStatusResponseProto getReplicationManagerStatus(
      ReplicationManagerStatusRequestProto request)
      throws IOException {
    return ReplicationManagerStatusResponseProto.newBuilder()
        .setIsRunning(impl.getReplicationManagerStatus()).build();
  }

}
