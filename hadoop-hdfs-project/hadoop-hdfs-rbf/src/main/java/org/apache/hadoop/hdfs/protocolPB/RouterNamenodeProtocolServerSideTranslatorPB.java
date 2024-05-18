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
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncRouterServer;

public class RouterNamenodeProtocolServerSideTranslatorPB
    extends NamenodeProtocolServerSideTranslatorPB{
  private final RouterRpcServer server;
  private final boolean isAsyncRpc;

  public RouterNamenodeProtocolServerSideTranslatorPB(NamenodeProtocol impl) {
    super(impl);
    this.server = (RouterRpcServer) impl;
    this.isAsyncRpc = server.isAsync();
  }


  @Override
  public NamenodeProtocolProtos.GetBlocksResponseProto getBlocks(
      RpcController unused,
      NamenodeProtocolProtos.GetBlocksRequestProto request) {
    if (!isAsyncRpc) {
      return getBlocks(unused, request);
    }
    asyncRouterServer(() -> {
      DatanodeInfo dnInfo = new DatanodeInfo.DatanodeInfoBuilder()
          .setNodeID(PBHelperClient.convert(request.getDatanode()))
          .build();
      return server.getBlocks(dnInfo, request.getSize(),
          request.getMinBlockSize(), request.getTimeInterval(),
          request.hasStorageType() ?
              PBHelperClient.convertStorageType(request.getStorageType()) : null);
    }, blocks ->
        NamenodeProtocolProtos.GetBlocksResponseProto.newBuilder()
        .setBlocks(PBHelper.convert(blocks)).build());
    return null;
  }

  @Override
  public NamenodeProtocolProtos.GetBlockKeysResponseProto getBlockKeys(
      RpcController unused,
      NamenodeProtocolProtos.GetBlockKeysRequestProto request) {
    if (!isAsyncRpc) {
      return getBlockKeys(unused, request);
    }
    asyncRouterServer(server::getBlockKeys, keys -> {
      NamenodeProtocolProtos.GetBlockKeysResponseProto.Builder builder =
          NamenodeProtocolProtos.GetBlockKeysResponseProto.newBuilder();
      if (keys != null) {
        builder.setKeys(PBHelper.convert(keys));
      }
      return builder.build();
    });
    return null;
  }

  @Override
  public NamenodeProtocolProtos.GetTransactionIdResponseProto getTransactionId(
      RpcController unused,
      NamenodeProtocolProtos.GetTransactionIdRequestProto request) {
    if (!isAsyncRpc) {
      return getTransactionId(unused, request);
    }
    asyncRouterServer(server::getTransactionID,
        txid -> NamenodeProtocolProtos
                .GetTransactionIdResponseProto
                .newBuilder().setTxId(txid).build());
    return null;
  }

  @Override
  public NamenodeProtocolProtos.GetMostRecentCheckpointTxIdResponseProto getMostRecentCheckpointTxId(
      RpcController unused, NamenodeProtocolProtos.GetMostRecentCheckpointTxIdRequestProto request) {
    if (!isAsyncRpc) {
      return getMostRecentCheckpointTxId(unused, request);
    }
    asyncRouterServer(server::getMostRecentCheckpointTxId,
        txid -> NamenodeProtocolProtos
            .GetMostRecentCheckpointTxIdResponseProto
            .newBuilder().setTxId(txid).build());
    return null;
  }

  @Override
  public NamenodeProtocolProtos.GetMostRecentNameNodeFileTxIdResponseProto getMostRecentNameNodeFileTxId(
      RpcController unused, NamenodeProtocolProtos.GetMostRecentNameNodeFileTxIdRequestProto request) {
    if (!isAsyncRpc) {
      return getMostRecentNameNodeFileTxId(unused, request);
    }
    asyncRouterServer(() -> server.getMostRecentNameNodeFileTxId(
        NNStorage.NameNodeFile.valueOf(request.getNameNodeFile())),
        txid -> NamenodeProtocolProtos
            .GetMostRecentNameNodeFileTxIdResponseProto
            .newBuilder().setTxId(txid).build());
    return null;
  }


  @Override
  public NamenodeProtocolProtos.RollEditLogResponseProto rollEditLog(
      RpcController unused,
      NamenodeProtocolProtos.RollEditLogRequestProto request) {
    if (!isAsyncRpc) {
      return rollEditLog(unused, request);
    }
    asyncRouterServer(server::rollEditLog,
        signature -> NamenodeProtocolProtos
            .RollEditLogResponseProto.newBuilder()
            .setSignature(PBHelper.convert(signature)).build());
    return null;
  }

  @Override
  public NamenodeProtocolProtos.ErrorReportResponseProto errorReport(
      RpcController unused,
      NamenodeProtocolProtos.ErrorReportRequestProto request) {
    if (!isAsyncRpc) {
      return errorReport(unused, request);
    }
    asyncRouterServer(() -> {
      server.errorReport(PBHelper.convert(request.getRegistration()),
          request.getErrorCode(), request.getMsg());
      return null;
    }, result -> VOID_ERROR_REPORT_RESPONSE);
    return null;
  }

  @Override
  public NamenodeProtocolProtos.RegisterResponseProto registerSubordinateNamenode(
      RpcController unused, NamenodeProtocolProtos.RegisterRequestProto request) {
    if (!isAsyncRpc) {
      return registerSubordinateNamenode(unused, request);
    }
    asyncRouterServer(() -> server.registerSubordinateNamenode(
        PBHelper.convert(request.getRegistration())),
        reg -> NamenodeProtocolProtos.RegisterResponseProto.newBuilder()
            .setRegistration(PBHelper.convert(reg)).build());
    return null;
  }

  @Override
  public NamenodeProtocolProtos.StartCheckpointResponseProto startCheckpoint(
      RpcController unused,
      NamenodeProtocolProtos.StartCheckpointRequestProto request) {
    if (!isAsyncRpc) {
      return startCheckpoint(unused, request);
    }
    asyncRouterServer(() ->
            server.startCheckpoint(PBHelper.convert(request.getRegistration())),
        cmd -> NamenodeProtocolProtos.StartCheckpointResponseProto.newBuilder()
        .setCommand(PBHelper.convert(cmd)).build());
    return null;
  }

  @Override
  public NamenodeProtocolProtos.EndCheckpointResponseProto endCheckpoint(
      RpcController unused,
      NamenodeProtocolProtos.EndCheckpointRequestProto request) {
    if (!isAsyncRpc) {
      return endCheckpoint(unused, request);
    }
    asyncRouterServer(() -> {
      server.endCheckpoint(PBHelper.convert(request.getRegistration()),
          PBHelper.convert(request.getSignature()));
      return null;
    }, result -> VOID_END_CHECKPOINT_RESPONSE);
    return null;
  }

  @Override
  public NamenodeProtocolProtos.GetEditLogManifestResponseProto getEditLogManifest(
      RpcController unused, NamenodeProtocolProtos.GetEditLogManifestRequestProto request) {
    if (!isAsyncRpc) {
      return getEditLogManifest(unused, request);
    }
    asyncRouterServer(() -> server.getEditLogManifest(request.getSinceTxId()),
        manifest -> NamenodeProtocolProtos
            .GetEditLogManifestResponseProto.newBuilder()
            .setManifest(PBHelper.convert(manifest)).build());
    return null;
  }

  @Override
  public HdfsServerProtos.VersionResponseProto versionRequest(
      RpcController controller,
      HdfsServerProtos.VersionRequestProto request) {
    if (!isAsyncRpc) {
      return versionRequest(controller, request);
    }
    asyncRouterServer(server::versionRequest,
        info -> HdfsServerProtos.VersionResponseProto.newBuilder()
        .setInfo(PBHelper.convert(info)).build());
    return null;
  }

  @Override
  public NamenodeProtocolProtos.IsUpgradeFinalizedResponseProto isUpgradeFinalized(
      RpcController controller, NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto request) {
    if (!isAsyncRpc) {
      return isUpgradeFinalized(controller, request);
    }
    asyncRouterServer(server::isUpgradeFinalized,
        isUpgradeFinalized -> NamenodeProtocolProtos
            .IsUpgradeFinalizedResponseProto.newBuilder()
            .setIsUpgradeFinalized(isUpgradeFinalized).build());
    return null;
  }

  @Override
  public NamenodeProtocolProtos.IsRollingUpgradeResponseProto isRollingUpgrade(
      RpcController controller, NamenodeProtocolProtos.IsRollingUpgradeRequestProto request)
      throws ServiceException {
    if (!isAsyncRpc) {
      return isRollingUpgrade(controller, request);
    }
    asyncRouterServer(server::isRollingUpgrade,
        isRollingUpgrade -> NamenodeProtocolProtos
            .IsRollingUpgradeResponseProto.newBuilder()
            .setIsRollingUpgrade(isRollingUpgrade).build());
    return null;
  }

  @Override
  public NamenodeProtocolProtos.GetNextSPSPathResponseProto getNextSPSPath(
      RpcController controller, NamenodeProtocolProtos.GetNextSPSPathRequestProto request) {
    if (!isAsyncRpc) {
      return getNextSPSPath(controller, request);
    }
    asyncRouterServer(server::getNextSPSPath,
        nextSPSPath -> NamenodeProtocolProtos
            .GetNextSPSPathResponseProto.newBuilder()
            .setSpsPath(nextSPSPath).build());
    return null;
  }
}
