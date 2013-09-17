/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocolPB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DeleteBlockPoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DeleteBlockPoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.RefreshNamenodesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.RefreshNamenodesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link ClientDatanodeProtocolPB} to the
 * {@link ClientDatanodeProtocol} server implementation.
 */
@InterfaceAudience.Private
public class ClientDatanodeProtocolServerSideTranslatorPB implements
    ClientDatanodeProtocolPB {
  private final static RefreshNamenodesResponseProto REFRESH_NAMENODE_RESP =
      RefreshNamenodesResponseProto.newBuilder().build();
  private final static DeleteBlockPoolResponseProto DELETE_BLOCKPOOL_RESP =
      DeleteBlockPoolResponseProto.newBuilder().build();
  
  private final ClientDatanodeProtocol impl;

  public ClientDatanodeProtocolServerSideTranslatorPB(
      ClientDatanodeProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetReplicaVisibleLengthResponseProto getReplicaVisibleLength(
      RpcController unused, GetReplicaVisibleLengthRequestProto request)
      throws ServiceException {
    long len;
    try {
      len = impl.getReplicaVisibleLength(PBHelper.convert(request.getBlock()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetReplicaVisibleLengthResponseProto.newBuilder().setLength(len)
        .build();
  }

  @Override
  public RefreshNamenodesResponseProto refreshNamenodes(
      RpcController unused, RefreshNamenodesRequestProto request)
      throws ServiceException {
    try {
      impl.refreshNamenodes();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return REFRESH_NAMENODE_RESP;
  }

  @Override
  public DeleteBlockPoolResponseProto deleteBlockPool(RpcController unused,
      DeleteBlockPoolRequestProto request) throws ServiceException {
    try {
      impl.deleteBlockPool(request.getBlockPool(), request.getForce());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return DELETE_BLOCKPOOL_RESP;
  }

  @Override
  public GetBlockLocalPathInfoResponseProto getBlockLocalPathInfo(
      RpcController unused, GetBlockLocalPathInfoRequestProto request)
      throws ServiceException {
    BlockLocalPathInfo resp;
    try {
      resp = impl.getBlockLocalPathInfo(PBHelper.convert(request.getBlock()), PBHelper.convert(request.getToken()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetBlockLocalPathInfoResponseProto.newBuilder()
        .setBlock(PBHelper.convert(resp.getBlock()))
        .setLocalPath(resp.getBlockPath()).setLocalMetaPath(resp.getMetaPath())
        .build();
  }

  @Override
  public GetHdfsBlockLocationsResponseProto getHdfsBlockLocations(
      RpcController controller, GetHdfsBlockLocationsRequestProto request)
      throws ServiceException {
    HdfsBlocksMetadata resp;
    try {
      // Construct the Lists to make the actual call
      List<ExtendedBlock> blocks = 
          new ArrayList<ExtendedBlock>(request.getBlocksCount());
      for (ExtendedBlockProto b : request.getBlocksList()) {
        blocks.add(PBHelper.convert(b));
      }
      List<Token<BlockTokenIdentifier>> tokens = 
          new ArrayList<Token<BlockTokenIdentifier>>(request.getTokensCount());
      for (TokenProto b : request.getTokensList()) {
        tokens.add(PBHelper.convert(b));
      }
      // Call the real implementation
      resp = impl.getHdfsBlocksMetadata(blocks, tokens);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    List<ByteString> volumeIdsByteStrings = 
        new ArrayList<ByteString>(resp.getVolumeIds().size());
    for (byte[] b : resp.getVolumeIds()) {
      volumeIdsByteStrings.add(ByteString.copyFrom(b));
    }
    // Build and return the response
    Builder builder = GetHdfsBlockLocationsResponseProto.newBuilder();
    builder.addAllVolumeIds(volumeIdsByteStrings);
    builder.addAllVolumeIndexes(resp.getVolumeIndexes());
    return builder.build();
  }
}
