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

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.VersionResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.EndCheckpointRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.EndCheckpointResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.ErrorReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentCheckpointTxIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentCheckpointTxIdResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetNextSPSPathRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetNextSPSPathResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetTransactionIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetTransactionIdResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsRollingUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsRollingUpgradeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsUpgradeFinalizedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RegisterRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RegisterResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RollEditLogRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RollEditLogResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.StartCheckpointRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.StartCheckpointResponseProto;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link NamenodeProtocolPB} to the
 * {@link NamenodeProtocol} server implementation.
 */
public class NamenodeProtocolServerSideTranslatorPB implements
    NamenodeProtocolPB {
  private final NamenodeProtocol impl;

  private final static ErrorReportResponseProto VOID_ERROR_REPORT_RESPONSE = 
  ErrorReportResponseProto.newBuilder().build();

  private final static EndCheckpointResponseProto VOID_END_CHECKPOINT_RESPONSE =
  EndCheckpointResponseProto.newBuilder().build();

  public NamenodeProtocolServerSideTranslatorPB(NamenodeProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetBlocksResponseProto getBlocks(RpcController unused,
      GetBlocksRequestProto request) throws ServiceException {
    DatanodeInfo dnInfo = new DatanodeInfoBuilder()
        .setNodeID(PBHelperClient.convert(request.getDatanode()))
        .build();
    BlocksWithLocations blocks;
    try {
      blocks = impl.getBlocks(dnInfo, request.getSize(),
          request.getMinBlockSize());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetBlocksResponseProto.newBuilder()
        .setBlocks(PBHelper.convert(blocks)).build();
  }

  @Override
  public GetBlockKeysResponseProto getBlockKeys(RpcController unused,
      GetBlockKeysRequestProto request) throws ServiceException {
    ExportedBlockKeys keys;
    try {
      keys = impl.getBlockKeys();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    GetBlockKeysResponseProto.Builder builder = 
        GetBlockKeysResponseProto.newBuilder();
    if (keys != null) {
      builder.setKeys(PBHelper.convert(keys));
    }
    return builder.build();
  }

  @Override
  public GetTransactionIdResponseProto getTransactionId(RpcController unused,
      GetTransactionIdRequestProto request) throws ServiceException {
    long txid;
    try {
      txid = impl.getTransactionID();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetTransactionIdResponseProto.newBuilder().setTxId(txid).build();
  }
  
  @Override
  public GetMostRecentCheckpointTxIdResponseProto getMostRecentCheckpointTxId(
      RpcController unused, GetMostRecentCheckpointTxIdRequestProto request)
      throws ServiceException {
    long txid;
    try {
      txid = impl.getMostRecentCheckpointTxId();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetMostRecentCheckpointTxIdResponseProto.newBuilder().setTxId(txid).build();
  }


  @Override
  public RollEditLogResponseProto rollEditLog(RpcController unused,
      RollEditLogRequestProto request) throws ServiceException {
    CheckpointSignature signature;
    try {
      signature = impl.rollEditLog();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return RollEditLogResponseProto.newBuilder()
        .setSignature(PBHelper.convert(signature)).build();
  }

  @Override
  public ErrorReportResponseProto errorReport(RpcController unused,
      ErrorReportRequestProto request) throws ServiceException {
    try {
      impl.errorReport(PBHelper.convert(request.getRegistration()),
          request.getErrorCode(), request.getMsg());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_ERROR_REPORT_RESPONSE;
  }

  @Override
  public RegisterResponseProto registerSubordinateNamenode(
      RpcController unused, RegisterRequestProto request)
      throws ServiceException {
    NamenodeRegistration reg;
    try {
      reg = impl.registerSubordinateNamenode(
          PBHelper.convert(request.getRegistration()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return RegisterResponseProto.newBuilder()
        .setRegistration(PBHelper.convert(reg)).build();
  }

  @Override
  public StartCheckpointResponseProto startCheckpoint(RpcController unused,
      StartCheckpointRequestProto request) throws ServiceException {
    NamenodeCommand cmd;
    try {
      cmd = impl.startCheckpoint(PBHelper.convert(request.getRegistration()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return StartCheckpointResponseProto.newBuilder()
        .setCommand(PBHelper.convert(cmd)).build();
  }

  @Override
  public EndCheckpointResponseProto endCheckpoint(RpcController unused,
      EndCheckpointRequestProto request) throws ServiceException {
    try {
      impl.endCheckpoint(PBHelper.convert(request.getRegistration()),
          PBHelper.convert(request.getSignature()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_END_CHECKPOINT_RESPONSE;
  }

  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(
      RpcController unused, GetEditLogManifestRequestProto request)
      throws ServiceException {
    RemoteEditLogManifest manifest;
    try {
      manifest = impl.getEditLogManifest(request.getSinceTxId());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetEditLogManifestResponseProto.newBuilder()
        .setManifest(PBHelper.convert(manifest)).build();
  }

  @Override
  public VersionResponseProto versionRequest(RpcController controller,
      VersionRequestProto request) throws ServiceException {
    NamespaceInfo info;
    try {
      info = impl.versionRequest();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VersionResponseProto.newBuilder()
        .setInfo(PBHelper.convert(info)).build();
  }

  @Override
  public IsUpgradeFinalizedResponseProto isUpgradeFinalized(
      RpcController controller, IsUpgradeFinalizedRequestProto request)
      throws ServiceException {
    boolean isUpgradeFinalized;
    try {
      isUpgradeFinalized = impl.isUpgradeFinalized();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return IsUpgradeFinalizedResponseProto.newBuilder()
        .setIsUpgradeFinalized(isUpgradeFinalized).build();
  }

  @Override
  public IsRollingUpgradeResponseProto isRollingUpgrade(
      RpcController controller, IsRollingUpgradeRequestProto request)
      throws ServiceException {
    boolean isRollingUpgrade;
    try {
      isRollingUpgrade = impl.isRollingUpgrade();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return IsRollingUpgradeResponseProto.newBuilder()
        .setIsRollingUpgrade(isRollingUpgrade).build();
  }

  @Override
  public GetNextSPSPathResponseProto getNextSPSPath(
      RpcController controller, GetNextSPSPathRequestProto request)
          throws ServiceException {
    try {
      Long nextSPSPath = impl.getNextSPSPath();
      if (nextSPSPath == null) {
        return GetNextSPSPathResponseProto.newBuilder().build();
      }
      return GetNextSPSPathResponseProto.newBuilder().setSpsPath(nextSPSPath)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
