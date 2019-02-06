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
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .AllocateScmBlockRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .AllocateScmBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .DeleteScmKeyBlocksRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .DeleteScmKeyBlocksResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .KeyBlocks;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
   * Asks SCM where a block should be allocated. SCM responds with the
   * set of datanodes that should be used creating this block.
   * @param size - size of the block.
   * @return allocated block accessing info (key, pipeline).
   * @throws IOException
   */
  @Override
  public AllocatedBlock allocateBlock(long size,
      HddsProtos.ReplicationType type, HddsProtos.ReplicationFactor factor,
      String owner) throws IOException {
    Preconditions.checkArgument(size > 0, "block size must be greater than 0");

    AllocateScmBlockRequestProto request =
        AllocateScmBlockRequestProto.newBuilder().setSize(size).setType(type)
            .setFactor(factor).setOwner(owner).build();
    final AllocateScmBlockResponseProto response;
    try {
      response = rpcProxy.allocateScmBlock(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    if (response.getErrorCode() !=
        AllocateScmBlockResponseProto.Error.success) {
      throw new IOException(response.hasErrorMessage() ?
          response.getErrorMessage() : "Allocate block failed.");
    }
    AllocatedBlock.Builder builder = new AllocatedBlock.Builder()
        .setContainerBlockID(
            ContainerBlockID.getFromProtobuf(response.getContainerBlockID()))
        .setPipeline(Pipeline.getFromProtobuf(response.getPipeline()));
    return builder.build();
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
        .newBuilder().addAllKeyBlocks(keyBlocksProto).build();

    final DeleteScmKeyBlocksResponseProto resp;
    try {
      resp = rpcProxy.deleteScmKeyBlocks(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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
    HddsProtos.GetScmInfoRespsonseProto resp;
    try {
      resp = rpcProxy.getScmInfo(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    ScmInfo.Builder builder = new ScmInfo.Builder()
        .setClusterId(resp.getClusterId())
        .setScmId(resp.getScmId());
    return builder.build();
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
