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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.util.concurrent.AsyncGet;

import java.io.IOException;
import org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.Response;
import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncIpc;
import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncResponse;

public class RouterNamenodeProtocolTranslatorPB extends NamenodeProtocolTranslatorPB{
  private final NamenodeProtocolPB rpcProxy;

  public RouterNamenodeProtocolTranslatorPB(NamenodeProtocolPB rpcProxy) {
    super(rpcProxy);
    this.rpcProxy = rpcProxy;
  }

  @Override
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size, long
      minBlockSize, long timeInterval, StorageType storageType)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getBlocks(datanode, size, minBlockSize, timeInterval, storageType);
    }
    NamenodeProtocolProtos.GetBlocksRequestProto.Builder builder =
        NamenodeProtocolProtos.GetBlocksRequestProto.newBuilder()
        .setDatanode(PBHelperClient.convert((DatanodeID)datanode)).setSize(size)
        .setMinBlockSize(minBlockSize).setTimeInterval(timeInterval);
    if (storageType != null) {
      builder.setStorageType(PBHelperClient.convertStorageType(storageType));
    }
    NamenodeProtocolProtos.GetBlocksRequestProto req = builder.build();

    AsyncGet<NamenodeProtocolProtos.GetBlocksResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.getBlocks(NULL_CONTROLLER, req));
    asyncResponse(() -> PBHelper.convert(
        asyncGet.get(-1, null).getBlocks()));
    return null;
  }

  @Override
  public ExportedBlockKeys getBlockKeys() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getBlockKeys();
    }
    AsyncGet<NamenodeProtocolProtos.GetBlockKeysResponseProto, Exception> asyncGet =
        asyncIpc(() ->
            rpcProxy.getBlockKeys(NULL_CONTROLLER, VOID_GET_BLOCKKEYS_REQUEST));
    asyncResponse(() -> {
      NamenodeProtocolProtos.GetBlockKeysResponseProto rsp =
          asyncGet.get(-1, null);
      return rsp.hasKeys() ? PBHelper.convert(rsp.getKeys()) : null;
    });
    return null;
  }

  @Override
  public long getTransactionID() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getTransactionID();
    }
    AsyncGet<NamenodeProtocolProtos.GetTransactionIdResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.getTransactionId(NULL_CONTROLLER,
            VOID_GET_TRANSACTIONID_REQUEST));
    asyncResponse(() -> asyncGet.get(-1, null).getTxId());
    return -1;
  }

  @Override
  public long getMostRecentCheckpointTxId() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getMostRecentCheckpointTxId();
    }
    AsyncGet<NamenodeProtocolProtos.GetMostRecentCheckpointTxIdResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.getMostRecentCheckpointTxId(NULL_CONTROLLER,
        NamenodeProtocolProtos.GetMostRecentCheckpointTxIdRequestProto.getDefaultInstance()));
    asyncResponse((Response<Object>) () -> asyncGet.get(-1, null).getTxId());
    return -1;
  }

  @Override
  public long getMostRecentNameNodeFileTxId(NNStorage.NameNodeFile nnf) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getMostRecentNameNodeFileTxId(nnf);
    }
    AsyncGet<NamenodeProtocolProtos.GetMostRecentNameNodeFileTxIdResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.getMostRecentNameNodeFileTxId(NULL_CONTROLLER,
        NamenodeProtocolProtos.GetMostRecentNameNodeFileTxIdRequestProto.newBuilder()
            .setNameNodeFile(nnf.toString()).build()));
    asyncResponse(() -> asyncGet.get(-1, null).getTxId());
    return -1;
  }

  @Override
  public CheckpointSignature rollEditLog() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.rollEditLog();
    }
    AsyncGet<NamenodeProtocolProtos.RollEditLogResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.rollEditLog(NULL_CONTROLLER,
        VOID_ROLL_EDITLOG_REQUEST));
    asyncResponse(() -> PBHelper.convert(asyncGet.get(-1, null).getSignature()));
    return null;
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.versionRequest();
    }
    AsyncGet<HdfsServerProtos.VersionResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.versionRequest(NULL_CONTROLLER,
        VOID_VERSION_REQUEST));
    asyncResponse(() -> PBHelper.convert(asyncGet.get(-1, null).getInfo()));
    return null;
  }

  @Override
  public void errorReport(NamenodeRegistration registration, int errorCode,
                          String msg) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.errorReport(registration, errorCode, msg);
      return;
    }
    NamenodeProtocolProtos.ErrorReportRequestProto req = NamenodeProtocolProtos.ErrorReportRequestProto.newBuilder()
        .setErrorCode(errorCode).setMsg(msg)
        .setRegistration(PBHelper.convert(registration)).build();
    AsyncGet<NamenodeProtocolProtos.ErrorReportResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.errorReport(NULL_CONTROLLER, req));
    asyncResponse(() -> {
      asyncGet.get(-1, null);
      return null;
    });
  }

  @Override
  public NamenodeRegistration registerSubordinateNamenode(
      NamenodeRegistration registration) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.registerSubordinateNamenode(registration);
    }
    NamenodeProtocolProtos.RegisterRequestProto req = NamenodeProtocolProtos.RegisterRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration)).build();
    AsyncGet<NamenodeProtocolProtos.RegisterResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.registerSubordinateNamenode(NULL_CONTROLLER, req));
    asyncResponse(() -> PBHelper.convert(asyncGet.get(-1, null).getRegistration()));
    return null;
  }

  @Override
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.startCheckpoint(registration);
    }
    NamenodeProtocolProtos.StartCheckpointRequestProto req = NamenodeProtocolProtos.StartCheckpointRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration)).build();
    AsyncGet<NamenodeProtocolProtos.StartCheckpointResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.startCheckpoint(NULL_CONTROLLER, req));
    asyncResponse(() -> {
      HdfsServerProtos.NamenodeCommandProto cmd =
          asyncGet.get(-1, null).getCommand();
      return PBHelper.convert(cmd);
    });
    return null;
  }

  @Override
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.endCheckpoint(registration, sig);
      return;
    }
    NamenodeProtocolProtos.EndCheckpointRequestProto req = NamenodeProtocolProtos.EndCheckpointRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration))
        .setSignature(PBHelper.convert(sig)).build();
    AsyncGet<NamenodeProtocolProtos.EndCheckpointResponseProto, Exception> asyncGet
        = asyncIpc(() -> rpcProxy.endCheckpoint(NULL_CONTROLLER, req));
    asyncResponse(() -> {
      asyncGet.get(-1, null);
      return null;
    });
  }

  @Override
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getEditLogManifest(sinceTxId);
    }
    NamenodeProtocolProtos.GetEditLogManifestRequestProto req = NamenodeProtocolProtos.GetEditLogManifestRequestProto
        .newBuilder().setSinceTxId(sinceTxId).build();
    AsyncGet<NamenodeProtocolProtos.GetEditLogManifestResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.getEditLogManifest(NULL_CONTROLLER, req));
    asyncResponse(() -> PBHelper.convert(asyncGet.get(-1, null).getManifest()));
    return null;
  }

  @Override
  public boolean isUpgradeFinalized() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.isUpgradeFinalized();
    }
    NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto req = NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto
        .newBuilder().build();
    AsyncGet<NamenodeProtocolProtos.IsUpgradeFinalizedResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.isUpgradeFinalized(NULL_CONTROLLER, req));
    asyncResponse(() -> asyncGet.get(-1, null).getIsUpgradeFinalized());
    return false;
  }

  @Override
  public boolean isRollingUpgrade() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.isRollingUpgrade();
    }
    NamenodeProtocolProtos.IsRollingUpgradeRequestProto req = NamenodeProtocolProtos.IsRollingUpgradeRequestProto
        .newBuilder().build();
    AsyncGet<NamenodeProtocolProtos.IsRollingUpgradeResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.isRollingUpgrade(NULL_CONTROLLER, req));
    asyncResponse(() -> asyncGet.get(-1, null).getIsRollingUpgrade());
    return false;
  }

  @Override
  public Long getNextSPSPath() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getNextSPSPath();
    }
    NamenodeProtocolProtos.GetNextSPSPathRequestProto req =
        NamenodeProtocolProtos.GetNextSPSPathRequestProto.newBuilder().build();
    AsyncGet<NamenodeProtocolProtos.GetNextSPSPathResponseProto, Exception> ayncGet =
        asyncIpc(() -> rpcProxy.getNextSPSPath(NULL_CONTROLLER, req));
    asyncResponse(() -> {
      NamenodeProtocolProtos.GetNextSPSPathResponseProto nextSPSPath =
          ayncGet.get(-1, null);
      return nextSPSPath.hasSpsPath() ? nextSPSPath.getSpsPath() : null;
    });
    return null;
  }
}
