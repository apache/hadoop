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
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SCMBlockLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SCMBlockLocationResponse;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmKeyBlocksRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmKeyBlocksResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.KeyBlocks;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .SortDatanodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .SortDatanodesResponseProto;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import static org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.Status.OK;

/**
 * This class is the client-side translator to translate the requests made on
 * the {@link ScmBlockLocationProtocol} interface to the RPC server
 * implementing {@link ScmBlockLocationProtocolPB}.
 */
@InterfaceAudience.Private
public final class ScmBlockLocationProtocolClientSideTranslatorPB
    implements ScmBlockLocationProtocol, ProtocolTranslator, Closeable {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final ScmBlockLocationProtocolPB rpcProxy;

  /**
   * Creates a new StorageContainerLocationProtocolClientSideTranslatorPB.
   *
   * @param rpcProxy {@link StorageContainerLocationProtocolPB} RPC proxy
   */
  public ScmBlockLocationProtocolClientSideTranslatorPB(
      ScmBlockLocationProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  /**
   * Returns a SCMBlockLocationRequest builder with specified type.
   * @param cmdType type of the request
   */
  private SCMBlockLocationRequest.Builder createSCMBlockRequest(Type cmdType) {
    return SCMBlockLocationRequest.newBuilder()
        .setCmdType(cmdType)
        .setTraceID(TracingUtil.exportCurrentSpan());
  }

  /**
   * Submits client request to SCM server.
   * @param req client request
   * @return response from SCM
   * @throws IOException thrown if any Protobuf service exception occurs
   */
  private SCMBlockLocationResponse submitRequest(
      SCMBlockLocationRequest req) throws IOException {
    try {
      SCMBlockLocationResponse response =
          rpcProxy.send(NULL_RPC_CONTROLLER, req);
      return response;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private SCMBlockLocationResponse handleError(SCMBlockLocationResponse resp)
      throws SCMException {
    if (resp.getStatus() != OK) {
      throw new SCMException(resp.getMessage(),
          SCMException.ResultCodes.values()[resp.getStatus().ordinal()]);
    }
    return resp;
  }

  /**
   * Asks SCM where a block should be allocated. SCM responds with the
   * set of datanodes that should be used creating this block.
   * @param size - size of the block.
   * @param num - number of blocks.
   * @param type - replication type of the blocks.
   * @param factor - replication factor of the blocks.
   * @param excludeList - exclude list while allocating blocks.
   * @return allocated block accessing info (key, pipeline).
   * @throws IOException
   */
  @Override
  public List<AllocatedBlock> allocateBlock(long size, int num,
      HddsProtos.ReplicationType type, HddsProtos.ReplicationFactor factor,
      String owner, ExcludeList excludeList) throws IOException {
    Preconditions.checkArgument(size > 0, "block size must be greater than 0");

    AllocateScmBlockRequestProto request =
        AllocateScmBlockRequestProto.newBuilder()
            .setSize(size)
            .setNumBlocks(num)
            .setType(type)
            .setFactor(factor)
            .setOwner(owner)
            .setExcludeList(excludeList.getProtoBuf())
            .build();

    SCMBlockLocationRequest wrapper = createSCMBlockRequest(
        Type.AllocateScmBlock)
        .setAllocateScmBlockRequest(request)
        .build();

    final SCMBlockLocationResponse wrappedResponse =
        handleError(submitRequest(wrapper));
    final AllocateScmBlockResponseProto response =
        wrappedResponse.getAllocateScmBlockResponse();

    List<AllocatedBlock> blocks = new ArrayList<>(response.getBlocksCount());
    for (AllocateBlockResponse resp : response.getBlocksList()) {
      AllocatedBlock.Builder builder = new AllocatedBlock.Builder()
          .setContainerBlockID(
              ContainerBlockID.getFromProtobuf(resp.getContainerBlockID()))
          .setPipeline(Pipeline.getFromProtobuf(resp.getPipeline()));
      blocks.add(builder.build());
    }

    return blocks;
  }

  /**
   * Delete the set of keys specified.
   *
   * @param keyBlocksInfoList batch of block keys to delete.
   * @return list of block deletion results.
   * @throws IOException if there is any failure.
   *
   */
  @Override
  public List<DeleteBlockGroupResult> deleteKeyBlocks(
      List<BlockGroup> keyBlocksInfoList) throws IOException {
    List<KeyBlocks> keyBlocksProto = keyBlocksInfoList.stream()
        .map(BlockGroup::getProto).collect(Collectors.toList());
    DeleteScmKeyBlocksRequestProto request = DeleteScmKeyBlocksRequestProto
        .newBuilder()
        .addAllKeyBlocks(keyBlocksProto)
        .build();

    SCMBlockLocationRequest wrapper = createSCMBlockRequest(
        Type.DeleteScmKeyBlocks)
        .setDeleteScmKeyBlocksRequest(request)
        .build();

    final SCMBlockLocationResponse wrappedResponse =
        handleError(submitRequest(wrapper));
    final DeleteScmKeyBlocksResponseProto resp =
        wrappedResponse.getDeleteScmKeyBlocksResponse();

    List<DeleteBlockGroupResult> results =
        new ArrayList<>(resp.getResultsCount());
    results.addAll(resp.getResultsList().stream().map(
        result -> new DeleteBlockGroupResult(result.getObjectKey(),
            DeleteBlockGroupResult
                .convertBlockResultProto(result.getBlockResultsList())))
        .collect(Collectors.toList()));
    return results;
  }

  /**
   * Gets the cluster Id and Scm Id from SCM.
   * @return ScmInfo
   * @throws IOException
   */
  @Override
  public ScmInfo getScmInfo() throws IOException {
    HddsProtos.GetScmInfoRequestProto request =
        HddsProtos.GetScmInfoRequestProto.getDefaultInstance();
    HddsProtos.GetScmInfoResponseProto resp;

    SCMBlockLocationRequest wrapper = createSCMBlockRequest(
        Type.GetScmInfo)
        .setGetScmInfoRequest(request)
        .build();

    final SCMBlockLocationResponse wrappedResponse =
        handleError(submitRequest(wrapper));
    resp = wrappedResponse.getGetScmInfoResponse();
    ScmInfo.Builder builder = new ScmInfo.Builder()
        .setClusterId(resp.getClusterId())
        .setScmId(resp.getScmId());
    return builder.build();
  }

  /**
   * Sort the datanodes based on distance from client.
   * @return List<DatanodeDetails></>
   * @throws IOException
   */
  @Override
  public List<DatanodeDetails> sortDatanodes(List<String> nodes,
      String clientMachine) throws IOException {
    SortDatanodesRequestProto request = SortDatanodesRequestProto
        .newBuilder()
        .addAllNodeNetworkName(nodes)
        .setClient(clientMachine)
        .build();
    SCMBlockLocationRequest wrapper = createSCMBlockRequest(
        Type.SortDatanodes)
        .setSortDatanodesRequest(request)
        .build();

    final SCMBlockLocationResponse wrappedResponse =
        handleError(submitRequest(wrapper));
    SortDatanodesResponseProto resp =
        wrappedResponse.getSortDatanodesResponse();
    List<DatanodeDetails> results = new ArrayList<>(resp.getNodeCount());
    results.addAll(resp.getNodeList().stream()
        .map(node -> DatanodeDetails.getFromProtoBuf(node))
        .collect(Collectors.toList()));
    return results;
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
