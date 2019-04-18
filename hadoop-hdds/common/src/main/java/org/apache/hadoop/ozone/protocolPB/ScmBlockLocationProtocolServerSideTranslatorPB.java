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
import io.opentracing.Scope;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .AllocateBlockResponse;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .AllocateScmBlockRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .AllocateScmBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .DeleteKeyBlocksResultProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .DeleteScmKeyBlocksRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos
    .DeleteScmKeyBlocksResponseProto;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link StorageContainerLocationProtocolPB} to the
 * {@link StorageContainerLocationProtocol} server implementation.
 */
@InterfaceAudience.Private
public final class ScmBlockLocationProtocolServerSideTranslatorPB
    implements ScmBlockLocationProtocolPB {

  private final ScmBlockLocationProtocol impl;

  /**
   * Creates a new ScmBlockLocationProtocolServerSideTranslatorPB.
   *
   * @param impl {@link ScmBlockLocationProtocol} server implementation
   */
  public ScmBlockLocationProtocolServerSideTranslatorPB(
      ScmBlockLocationProtocol impl) throws IOException {
    this.impl = impl;
  }

  @Override
  public AllocateScmBlockResponseProto allocateScmBlock(
      RpcController controller, AllocateScmBlockRequestProto request)
      throws ServiceException {
    try (Scope scope = TracingUtil
        .importAndCreateScope("ScmBlockLocationProtocol.allocateBlock",
            request.getTraceID())) {
      List<AllocatedBlock> allocatedBlocks =
          impl.allocateBlock(request.getSize(),
              request.getNumBlocks(), request.getType(),
              request.getFactor(), request.getOwner(),
              ExcludeList.getFromProtoBuf(request.getExcludeList()));

      AllocateScmBlockResponseProto.Builder builder =
          AllocateScmBlockResponseProto.newBuilder();

      if (allocatedBlocks.size() < request.getNumBlocks()) {
        return builder
            .setErrorCode(AllocateScmBlockResponseProto.Error.unknownFailure)
            .build();
      }

      for (AllocatedBlock block : allocatedBlocks) {
        builder.addBlocks(AllocateBlockResponse.newBuilder()
            .setContainerBlockID(block.getBlockID().getProtobuf())
            .setPipeline(block.getPipeline().getProtobufMessage()));
      }

      return builder
          .setErrorCode(AllocateScmBlockResponseProto.Error.success)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DeleteScmKeyBlocksResponseProto deleteScmKeyBlocks(
      RpcController controller, DeleteScmKeyBlocksRequestProto req)
      throws ServiceException {
    DeleteScmKeyBlocksResponseProto.Builder resp =
        DeleteScmKeyBlocksResponseProto.newBuilder();
    try {
      List<BlockGroup> infoList = req.getKeyBlocksList().stream()
          .map(BlockGroup::getFromProto).collect(Collectors.toList());
      final List<DeleteBlockGroupResult> results =
          impl.deleteKeyBlocks(infoList);
      for (DeleteBlockGroupResult result: results) {
        DeleteKeyBlocksResultProto.Builder deleteResult =
            DeleteKeyBlocksResultProto
            .newBuilder()
            .setObjectKey(result.getObjectKey())
            .addAllBlockResults(result.getBlockResultProtoList());
        resp.addResults(deleteResult.build());
      }
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
    return resp.build();
  }

  @Override
  public HddsProtos.GetScmInfoResponseProto getScmInfo(
      RpcController controller, HddsProtos.GetScmInfoRequestProto req)
      throws ServiceException {
    ScmInfo scmInfo;
    try {
      scmInfo = impl.getScmInfo();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
    return HddsProtos.GetScmInfoResponseProto.newBuilder()
        .setClusterId(scmInfo.getClusterId())
        .setScmId(scmInfo.getScmId())
        .build();
  }
}
