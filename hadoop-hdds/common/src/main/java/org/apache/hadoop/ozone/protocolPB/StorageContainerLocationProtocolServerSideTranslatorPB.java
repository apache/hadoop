
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.InChillModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.InChillModeResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.ForceExitChillModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.ForceExitChillModeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineResponseProto;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.ContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.ContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.GetContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.GetContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.ObjectStageChangeRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.ObjectStageChangeResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.PipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.PipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.SCMDeleteContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.SCMDeleteContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.SCMListContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.SCMListContainerResponseProto;

import java.io.IOException;
import java.util.List;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link StorageContainerLocationProtocolPB} to the
 * {@link StorageContainerLocationProtocol} server implementation.
 */
@InterfaceAudience.Private
public final class StorageContainerLocationProtocolServerSideTranslatorPB
    implements StorageContainerLocationProtocolPB {

  private final StorageContainerLocationProtocol impl;

  /**
   * Creates a new StorageContainerLocationProtocolServerSideTranslatorPB.
   *
   * @param impl {@link StorageContainerLocationProtocol} server implementation
   */
  public StorageContainerLocationProtocolServerSideTranslatorPB(
      StorageContainerLocationProtocol impl) throws IOException {
    this.impl = impl;
  }

  @Override
  public ContainerResponseProto allocateContainer(RpcController unused,
      ContainerRequestProto request) throws ServiceException {
    try {
      ContainerWithPipeline containerWithPipeline = impl
          .allocateContainer(request.getReplicationType(),
              request.getReplicationFactor(), request.getOwner());
      return ContainerResponseProto.newBuilder()
          .setContainerWithPipeline(containerWithPipeline.getProtobuf())
          .setErrorCode(ContainerResponseProto.Error.success)
          .build();

    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetContainerResponseProto getContainer(
      RpcController controller, GetContainerRequestProto request)
      throws ServiceException {
    try {
      ContainerInfo container = impl.getContainer(request.getContainerID());
      return GetContainerResponseProto.newBuilder()
          .setContainerInfo(container.getProtobuf())
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetContainerWithPipelineResponseProto getContainerWithPipeline(
      RpcController controller, GetContainerWithPipelineRequestProto request)
      throws ServiceException {
    try {
      ContainerWithPipeline container = impl
          .getContainerWithPipeline(request.getContainerID());
      return GetContainerWithPipelineResponseProto.newBuilder()
          .setContainerWithPipeline(container.getProtobuf())
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SCMListContainerResponseProto listContainer(RpcController controller,
      SCMListContainerRequestProto request) throws ServiceException {
    try {
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
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SCMDeleteContainerResponseProto deleteContainer(
      RpcController controller, SCMDeleteContainerRequestProto request)
      throws ServiceException {
    try {
      impl.deleteContainer(request.getContainerID());
      return SCMDeleteContainerResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StorageContainerLocationProtocolProtos.NodeQueryResponseProto
      queryNode(RpcController controller,
      StorageContainerLocationProtocolProtos.NodeQueryRequestProto request)
      throws ServiceException {
    try {
      HddsProtos.NodeState nodeState = request.getState();
      List<HddsProtos.Node> datanodes = impl.queryNode(nodeState,
          request.getScope(), request.getPoolName());
      return StorageContainerLocationProtocolProtos
          .NodeQueryResponseProto.newBuilder()
          .addAllDatanodes(datanodes)
          .build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ObjectStageChangeResponseProto notifyObjectStageChange(
      RpcController controller, ObjectStageChangeRequestProto request)
      throws ServiceException {
    try {
      impl.notifyObjectStageChange(request.getType(), request.getId(),
          request.getOp(), request.getStage());
      return ObjectStageChangeResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public PipelineResponseProto allocatePipeline(
      RpcController controller, PipelineRequestProto request)
      throws ServiceException {
    // TODO : Wiring this up requires one more patch.
    return null;
  }

  @Override
  public HddsProtos.GetScmInfoRespsonseProto getScmInfo(
      RpcController controller, HddsProtos.GetScmInfoRequestProto req)
      throws ServiceException {
    try {
      ScmInfo scmInfo = impl.getScmInfo();
      return HddsProtos.GetScmInfoRespsonseProto.newBuilder()
          .setClusterId(scmInfo.getClusterId())
          .setScmId(scmInfo.getScmId())
          .build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }

  }

  @Override
  public InChillModeResponseProto inChillMode(
      RpcController controller,
      InChillModeRequestProto request) throws ServiceException {
    try {
      return InChillModeResponseProto.newBuilder()
          .setInChillMode(impl.inChillMode()).build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ForceExitChillModeResponseProto forceExitChillMode(
      RpcController controller, ForceExitChillModeRequestProto request)
      throws ServiceException {
    try {
      return ForceExitChillModeResponseProto.newBuilder()
          .setExitedChillMode(impl.forceExitChillMode()).build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }
}
