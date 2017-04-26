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
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.DatanodeVolumeInfo;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DeleteBlockPoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DeleteBlockPoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.EvictWritersRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.EvictWritersResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBalancerBandwidthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBalancerBandwidthResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetDatanodeInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetDatanodeInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetHdfsBlockLocationsResponseProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetVolumeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetVolumeReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.RefreshNamenodesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.RefreshNamenodesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ShutdownDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ShutdownDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.StartReconfigurationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.StartReconfigurationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.TriggerBlockReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.TriggerBlockReportResponseProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeVolumeInfoProto;
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
  private final static ShutdownDatanodeResponseProto SHUTDOWN_DATANODE_RESP =
      ShutdownDatanodeResponseProto.newBuilder().build();
  private final static StartReconfigurationResponseProto START_RECONFIG_RESP =
      StartReconfigurationResponseProto.newBuilder().build();
  private final static TriggerBlockReportResponseProto TRIGGER_BLOCK_REPORT_RESP =
      TriggerBlockReportResponseProto.newBuilder().build();
  private final static EvictWritersResponseProto EVICT_WRITERS_RESP =
      EvictWritersResponseProto.newBuilder().build();
  
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
      len = impl.getReplicaVisibleLength(PBHelperClient.convert(request.getBlock()));
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
      resp = impl.getBlockLocalPathInfo(
                 PBHelperClient.convert(request.getBlock()),
                 PBHelperClient.convert(request.getToken()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetBlockLocalPathInfoResponseProto.newBuilder()
        .setBlock(PBHelperClient.convert(resp.getBlock()))
        .setLocalPath(resp.getBlockPath()).setLocalMetaPath(resp.getMetaPath())
        .build();
  }

  @Override
  public GetHdfsBlockLocationsResponseProto getHdfsBlockLocations(
      RpcController controller, GetHdfsBlockLocationsRequestProto request)
      throws ServiceException {
    HdfsBlocksMetadata resp;
    try {
      String poolId = request.getBlockPoolId();

      List<Token<BlockTokenIdentifier>> tokens =
          new ArrayList<Token<BlockTokenIdentifier>>(request.getTokensCount());
      for (TokenProto b : request.getTokensList()) {
        tokens.add(PBHelperClient.convert(b));
      }
      long[] blockIds = Longs.toArray(request.getBlockIdsList());
      
      // Call the real implementation
      resp = impl.getHdfsBlocksMetadata(poolId, blockIds, tokens);
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

  @Override
  public ShutdownDatanodeResponseProto shutdownDatanode(
      RpcController unused, ShutdownDatanodeRequestProto request)
      throws ServiceException {
    try {
      impl.shutdownDatanode(request.getForUpgrade());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return SHUTDOWN_DATANODE_RESP;
  }

  @Override
  public EvictWritersResponseProto evictWriters(RpcController unused,
      EvictWritersRequestProto request) throws ServiceException {
    try {
      impl.evictWriters();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return EVICT_WRITERS_RESP;
  }

  public GetDatanodeInfoResponseProto getDatanodeInfo(RpcController unused,
      GetDatanodeInfoRequestProto request) throws ServiceException {
    GetDatanodeInfoResponseProto res;
    try {
      res = GetDatanodeInfoResponseProto.newBuilder()
          .setLocalInfo(PBHelperClient.convert(impl.getDatanodeInfo())).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return res;
  }

  @Override
  public StartReconfigurationResponseProto startReconfiguration(
      RpcController unused, StartReconfigurationRequestProto request)
      throws ServiceException {
    try {
      impl.startReconfiguration();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return START_RECONFIG_RESP;
  }

  @Override
  public ListReconfigurablePropertiesResponseProto listReconfigurableProperties(
      RpcController controller,
      ListReconfigurablePropertiesRequestProto request)
      throws ServiceException {
    try {
      return ReconfigurationProtocolServerSideUtils
          .listReconfigurableProperties(impl.listReconfigurableProperties());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetReconfigurationStatusResponseProto getReconfigurationStatus(
      RpcController unused, GetReconfigurationStatusRequestProto request)
      throws ServiceException {
    try {
      return ReconfigurationProtocolServerSideUtils
          .getReconfigurationStatus(impl.getReconfigurationStatus());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public TriggerBlockReportResponseProto triggerBlockReport(
      RpcController unused, TriggerBlockReportRequestProto request)
          throws ServiceException {
    try {
      impl.triggerBlockReport(new BlockReportOptions.Factory().
          setIncremental(request.getIncremental()).build());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return TRIGGER_BLOCK_REPORT_RESP;
  }

  @Override
  public GetBalancerBandwidthResponseProto getBalancerBandwidth(
      RpcController controller, GetBalancerBandwidthRequestProto request)
      throws ServiceException {
    long bandwidth;
    try {
      bandwidth = impl.getBalancerBandwidth();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetBalancerBandwidthResponseProto.newBuilder()
        .setBandwidth(bandwidth).build();
  }

  @Override
  public GetVolumeReportResponseProto getVolumeReport(RpcController controller,
      GetVolumeReportRequestProto request) throws ServiceException {
    try {
      org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.
          GetVolumeReportResponseProto.Builder builder =
          GetVolumeReportResponseProto.newBuilder();
      List<DatanodeVolumeInfo> volumeReport = impl.getVolumeReport();
      for (DatanodeVolumeInfo info : volumeReport) {
        builder.addVolumeInfo(DatanodeVolumeInfoProto.newBuilder()
            .setPath(info.getPath()).setFreeSpace(info.getFreeSpace())
            .setNumBlocks(info.getNumBlocks())
            .setReservedSpace(info.getReservedSpace())
            .setReservedSpaceForReplicas(info.getReservedSpaceForReplicas())
            .setStorageType(
                PBHelperClient.convertStorageType(info.getStorageType()))
            .setUsedSpace(info.getUsedSpace()));
      }
      return builder.build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }
}
