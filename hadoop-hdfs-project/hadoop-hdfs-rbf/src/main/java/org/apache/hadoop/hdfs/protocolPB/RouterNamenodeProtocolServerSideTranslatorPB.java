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

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
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
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentNameNodeFileTxIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentNameNodeFileTxIdResponseProto;
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
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncRouterServer;

public class RouterNamenodeProtocolServerSideTranslatorPB
    extends NamenodeProtocolServerSideTranslatorPB {

  private final RouterRpcServer server;
  private final boolean isAsyncRpc;

  public RouterNamenodeProtocolServerSideTranslatorPB(NamenodeProtocol impl) {
    super(impl);
    this.server = (RouterRpcServer) impl;
    this.isAsyncRpc = server.isAsync();
  }

  @Override
  public GetBlocksResponseProto getBlocks(RpcController unused,
      GetBlocksRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.getBlocks(unused, request);
    }
    asyncRouterServer(() -> {
      DatanodeInfo dnInfo = new DatanodeInfo.DatanodeInfoBuilder()
          .setNodeID(PBHelperClient.convert(request.getDatanode()))
          .build();
      return server.getBlocks(dnInfo, request.getSize(),
          request.getMinBlockSize(), request.getTimeInterval(),
          request.hasStorageType() ?
              PBHelperClient.convertStorageType(request.getStorageType()): null);
    }, blocks ->
        GetBlocksResponseProto.newBuilder()
            .setBlocks(PBHelper.convert(blocks)).build());
    return null;
  }

  @Override
  public GetBlockKeysResponseProto getBlockKeys(RpcController unused,
      GetBlockKeysRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.getBlockKeys(unused, request);
    }
    asyncRouterServer(server::getBlockKeys, keys -> {
      GetBlockKeysResponseProto.Builder builder =
          GetBlockKeysResponseProto.newBuilder();
      if (keys != null) {
        builder.setKeys(PBHelper.convert(keys));
      }
      return builder.build();
    });
    return null;
  }

  @Override
  public GetTransactionIdResponseProto getTransactionId(RpcController unused,
      GetTransactionIdRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.getTransactionId(unused, request);
    }
    asyncRouterServer(server::getTransactionID,
        txid -> GetTransactionIdResponseProto
            .newBuilder().setTxId(txid).build());
    return null;
  }

  @Override
  public GetMostRecentCheckpointTxIdResponseProto getMostRecentCheckpointTxId(
      RpcController unused, GetMostRecentCheckpointTxIdRequestProto request)
      throws ServiceException {
    if (!isAsyncRpc) {
      return super.getMostRecentCheckpointTxId(unused, request);
    }
    asyncRouterServer(server::getMostRecentCheckpointTxId,
        txid -> GetMostRecentCheckpointTxIdResponseProto
            .newBuilder().setTxId(txid).build());
    return null;
  }

  @Override
  public GetMostRecentNameNodeFileTxIdResponseProto getMostRecentNameNodeFileTxId(
      RpcController unused, GetMostRecentNameNodeFileTxIdRequestProto request)
      throws ServiceException {
    if (!isAsyncRpc) {
      return super.getMostRecentNameNodeFileTxId(unused, request);
    }
    asyncRouterServer(() -> server.getMostRecentNameNodeFileTxId(
        NNStorage.NameNodeFile.valueOf(request.getNameNodeFile())),
        txid -> GetMostRecentNameNodeFileTxIdResponseProto
            .newBuilder().setTxId(txid).build());
    return null;
  }

  @Override
  public RollEditLogResponseProto rollEditLog(RpcController unused,
      RollEditLogRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.rollEditLog(unused, request);
    }
    asyncRouterServer(server::rollEditLog,
        signature -> RollEditLogResponseProto.newBuilder()
            .setSignature(PBHelper.convert(signature)).build());
    return null;
  }

  @Override
  public ErrorReportResponseProto errorReport(RpcController unused,
      ErrorReportRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.errorReport(unused, request);
    }
    asyncRouterServer(() -> {
      server.errorReport(PBHelper.convert(request.getRegistration()),
          request.getErrorCode(), request.getMsg());
      return null;
    }, result -> VOID_ERROR_REPORT_RESPONSE);
    return null;
  }

  @Override
  public RegisterResponseProto registerSubordinateNamenode(
      RpcController unused, RegisterRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.registerSubordinateNamenode(unused, request);
    }
    asyncRouterServer(() -> server.registerSubordinateNamenode(
        PBHelper.convert(request.getRegistration())),
        reg -> RegisterResponseProto.newBuilder()
            .setRegistration(PBHelper.convert(reg)).build());
    return null;
  }

  @Override
  public StartCheckpointResponseProto startCheckpoint(RpcController unused,
      StartCheckpointRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.startCheckpoint(unused, request);
    }
    asyncRouterServer(() ->
            server.startCheckpoint(PBHelper.convert(request.getRegistration())),
        cmd -> StartCheckpointResponseProto.newBuilder()
            .setCommand(PBHelper.convert(cmd)).build());
    return null;
  }


  @Override
  public EndCheckpointResponseProto endCheckpoint(RpcController unused,
      EndCheckpointRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.endCheckpoint(unused, request);
    }
    asyncRouterServer(() -> {
      server.endCheckpoint(PBHelper.convert(request.getRegistration()),
          PBHelper.convert(request.getSignature()));
      return null;
    }, result -> VOID_END_CHECKPOINT_RESPONSE);
    return null;
  }

  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(
      RpcController unused, GetEditLogManifestRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.getEditLogManifest(unused, request);
    }
    asyncRouterServer(() -> server.getEditLogManifest(request.getSinceTxId()),
        manifest -> GetEditLogManifestResponseProto.newBuilder()
            .setManifest(PBHelper.convert(manifest)).build());
    return null;
  }

  @Override
  public VersionResponseProto versionRequest(
      RpcController controller,
      VersionRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.versionRequest(controller, request);
    }
    asyncRouterServer(server::versionRequest,
        info -> VersionResponseProto.newBuilder()
            .setInfo(PBHelper.convert(info)).build());
    return null;
  }

  @Override
  public IsUpgradeFinalizedResponseProto isUpgradeFinalized(RpcController controller,
      IsUpgradeFinalizedRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.isUpgradeFinalized(controller, request);
    }
    asyncRouterServer(server::isUpgradeFinalized,
        isUpgradeFinalized -> IsUpgradeFinalizedResponseProto.newBuilder()
            .setIsUpgradeFinalized(isUpgradeFinalized).build());
    return null;
  }

  @Override
  public IsRollingUpgradeResponseProto isRollingUpgrade(
      RpcController controller, IsRollingUpgradeRequestProto request)
      throws ServiceException {
    if (!isAsyncRpc) {
      return super.isRollingUpgrade(controller, request);
    }
    asyncRouterServer(server::isRollingUpgrade,
        isRollingUpgrade -> IsRollingUpgradeResponseProto.newBuilder()
            .setIsRollingUpgrade(isRollingUpgrade).build());
    return null;
  }

  @Override
  public GetNextSPSPathResponseProto getNextSPSPath(
      RpcController controller, GetNextSPSPathRequestProto request)
      throws ServiceException {
    if (!isAsyncRpc) {
      return super.getNextSPSPath(controller, request);
    }
    asyncRouterServer(server::getNextSPSPath,
        nextSPSPath -> GetNextSPSPathResponseProto.newBuilder()
            .setSpsPath(nextSPSPath).build());
    return null;
  }
}
