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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmKeyBlocksRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmKeyBlocksResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.KeyBlocks;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

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
            .setTraceID(TracingUtil.exportCurrentSpan())
            .setExcludeList(excludeList.getProtoBuf())
            .build();
    final AllocateScmBlockResponseProto response;
    try {
      response = rpcProxy.allocateScmBlock(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw transformServiceException(e);
    }
    if (response.getErrorCode() !=
        AllocateScmBlockResponseProto.Error.success) {
      throw new IOException(response.hasErrorMessage() ?
          response.getErrorMessage() : "Allocate block failed.");
    }

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

    final DeleteScmKeyBlocksResponseProto resp;
    try {
      resp = rpcProxy.deleteScmKeyBlocks(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw transformServiceException(e);
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

  private IOException transformServiceException(
      ServiceException se) throws IOException {
    //TODO SCM has no perfect way to return with business exceptions. All
    //the exceptions will be mapped to ServiceException.
    //ServiceException is handled in a special way in hadoop rpc: the message
    //contains the whole stack trace which is not required for the business
    //exception. As of now I remove the stack trace (use first line only).
    //Long term we need a proper way of the exception propagation.
    Throwable cause = se.getCause();
    if (cause == null) {
      return new IOException(
          new ServiceException(useFirstLine(se.getMessage()), se.getCause()));
    }
    return new IOException(useFirstLine(cause.getMessage()), cause.getCause());
  }

  private String useFirstLine(String message) {
    if (message == null) {
      return null;
    } else {
      return message.split("\n")[0];
    }
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
    try {
      resp = rpcProxy.getScmInfo(NULL_RPC_CONTROLLER, request);
    } catch (ServiceException e) {
      throw transformServiceException(e);
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
