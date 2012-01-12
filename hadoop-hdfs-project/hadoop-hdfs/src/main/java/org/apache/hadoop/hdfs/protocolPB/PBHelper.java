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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockKeyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockTokenIdentifierProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlocksWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CheckpointCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CheckpointSignatureProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto.AdminState;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExportedBlockKeysProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamespaceInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RecoveringBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogManifestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ReplicaStateProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.security.token.Token;

import com.google.protobuf.ByteString;

/**
 * Utilities for converting protobuf classes to and from implementation classes.
 */
class PBHelper {
  private PBHelper() {
    /** Hidden constructor */
  }

  public static ByteString getByteString(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }

  public static NamenodeRole convert(NamenodeRoleProto role) {
    switch (role) {
    case NAMENODE:
      return NamenodeRole.NAMENODE;
    case BACKUP:
      return NamenodeRole.BACKUP;
    case CHECKPOINT:
      return NamenodeRole.CHECKPOINT;
    }
    return null;
  }

  public static NamenodeRoleProto convert(NamenodeRole role) {
    switch (role) {
    case NAMENODE:
      return NamenodeRoleProto.NAMENODE;
    case BACKUP:
      return NamenodeRoleProto.BACKUP;
    case CHECKPOINT:
      return NamenodeRoleProto.CHECKPOINT;
    }
    return null;
  }

  public static StorageInfoProto convert(StorageInfo info) {
    return StorageInfoProto.newBuilder().setClusterID(info.getClusterID())
        .setCTime(info.getCTime()).setLayoutVersion(info.getLayoutVersion())
        .setNamespceID(info.getNamespaceID()).build();
  }

  public static StorageInfo convert(StorageInfoProto info) {
    return new StorageInfo(info.getLayoutVersion(), info.getNamespceID(),
        info.getClusterID(), info.getCTime());
  }

  public static NamenodeRegistrationProto convert(NamenodeRegistration reg) {
    return NamenodeRegistrationProto.newBuilder()
        .setHttpAddress(reg.getHttpAddress()).setRole(convert(reg.getRole()))
        .setRpcAddress(reg.getAddress())
        .setStorageInfo(convert((StorageInfo) reg)).build();
  }

  public static NamenodeRegistration convert(NamenodeRegistrationProto reg) {
    return new NamenodeRegistration(reg.getRpcAddress(), reg.getHttpAddress(),
        convert(reg.getStorageInfo()), convert(reg.getRole()));
  }

  public static DatanodeID convert(DatanodeIDProto dn) {
    return new DatanodeID(dn.getName(), dn.getStorageID(), dn.getInfoPort(),
        dn.getIpcPort());
  }

  public static DatanodeIDProto convert(DatanodeID dn) {
    return DatanodeIDProto.newBuilder().setName(dn.getName())
        .setInfoPort(dn.getInfoPort()).setIpcPort(dn.getIpcPort())
        .setStorageID(dn.getStorageID()).build();
  }

  public static BlockProto convert(Block b) {
    return BlockProto.newBuilder().setBlockId(b.getBlockId())
        .setGenStamp(b.getGenerationStamp()).setNumBytes(b.getNumBytes())
        .build();
  }

  public static Block convert(BlockProto b) {
    return new Block(b.getBlockId(), b.getGenStamp(), b.getNumBytes());
  }

  public static BlockWithLocationsProto convert(BlockWithLocations blk) {
    return BlockWithLocationsProto.newBuilder()
        .setBlock(convert(blk.getBlock()))
        .addAllDatanodeIDs(Arrays.asList(blk.getDatanodes())).build();
  }

  public static BlockWithLocations convert(BlockWithLocationsProto b) {
    return new BlockWithLocations(convert(b.getBlock()), b.getDatanodeIDsList()
        .toArray(new String[0]));
  }

  public static BlocksWithLocationsProto convert(BlocksWithLocations blks) {
    BlocksWithLocationsProto.Builder builder = BlocksWithLocationsProto
        .newBuilder();
    for (BlockWithLocations b : blks.getBlocks()) {
      builder.addBlocks(convert(b));
    }
    return builder.build();
  }

  public static BlocksWithLocations convert(BlocksWithLocationsProto blocks) {
    List<BlockWithLocationsProto> b = blocks.getBlocksList();
    BlockWithLocations[] ret = new BlockWithLocations[b.size()];
    int i = 0;
    for (BlockWithLocationsProto entry : b) {
      ret[i++] = convert(entry);
    }
    return new BlocksWithLocations(ret);
  }

  public static BlockKeyProto convert(BlockKey key) {
    byte[] encodedKey = key.getEncodedKey();
    ByteString keyBytes = ByteString.copyFrom(encodedKey == null ? new byte[0]
        : encodedKey);
    return BlockKeyProto.newBuilder().setKeyId(key.getKeyId())
        .setKeyBytes(keyBytes).setExpiryDate(key.getExpiryDate()).build();
  }

  public static BlockKey convert(BlockKeyProto k) {
    return new BlockKey(k.getKeyId(), k.getExpiryDate(), k.getKeyBytes()
        .toByteArray());
  }

  public static ExportedBlockKeysProto convert(ExportedBlockKeys keys) {
    ExportedBlockKeysProto.Builder builder = ExportedBlockKeysProto
        .newBuilder();
    builder.setIsBlockTokenEnabled(keys.isBlockTokenEnabled())
        .setKeyUpdateInterval(keys.getKeyUpdateInterval())
        .setTokenLifeTime(keys.getTokenLifetime())
        .setCurrentKey(convert(keys.getCurrentKey()));
    for (BlockKey k : keys.getAllKeys()) {
      builder.addAllKeys(convert(k));
    }
    return builder.build();
  }

  public static ExportedBlockKeys convert(ExportedBlockKeysProto keys) {
    return new ExportedBlockKeys(keys.getIsBlockTokenEnabled(),
        keys.getKeyUpdateInterval(), keys.getTokenLifeTime(),
        convert(keys.getCurrentKey()), convertBlockKeys(keys.getAllKeysList()));
  }

  public static CheckpointSignatureProto convert(CheckpointSignature s) {
    return CheckpointSignatureProto.newBuilder()
        .setBlockPoolId(s.getBlockpoolID())
        .setCurSegmentTxId(s.getCurSegmentTxId())
        .setMostRecentCheckpointTxId(s.getMostRecentCheckpointTxId())
        .setStorageInfo(PBHelper.convert((StorageInfo) s)).build();
  }

  public static CheckpointSignature convert(CheckpointSignatureProto s) {
    return new CheckpointSignature(PBHelper.convert(s.getStorageInfo()),
        s.getBlockPoolId(), s.getMostRecentCheckpointTxId(),
        s.getCurSegmentTxId());
  }

  public static RemoteEditLogProto convert(RemoteEditLog log) {
    return RemoteEditLogProto.newBuilder().setEndTxId(log.getEndTxId())
        .setStartTxId(log.getStartTxId()).build();
  }

  public static RemoteEditLog convert(RemoteEditLogProto l) {
    return new RemoteEditLog(l.getStartTxId(), l.getEndTxId());
  }

  public static RemoteEditLogManifestProto convert(
      RemoteEditLogManifest manifest) {
    RemoteEditLogManifestProto.Builder builder = RemoteEditLogManifestProto
        .newBuilder();
    for (RemoteEditLog log : manifest.getLogs()) {
      builder.addLogs(convert(log));
    }
    return builder.build();
  }

  public static RemoteEditLogManifest convert(
      RemoteEditLogManifestProto manifest) {
    List<RemoteEditLog> logs = new ArrayList<RemoteEditLog>(manifest
        .getLogsList().size());
    for (RemoteEditLogProto l : manifest.getLogsList()) {
      logs.add(convert(l));
    }
    return new RemoteEditLogManifest(logs);
  }

  public static CheckpointCommandProto convert(CheckpointCommand cmd) {
    return CheckpointCommandProto.newBuilder()
        .setSignature(convert(cmd.getSignature())).build();
  }

  public static NamenodeCommandProto convert(NamenodeCommand cmd) {
    if (cmd instanceof CheckpointCommand) {
      return NamenodeCommandProto.newBuilder().setAction(cmd.getAction())
          .setType(NamenodeCommandProto.Type.NamenodeCommand)
          .setCheckpointCmd(convert((CheckpointCommand) cmd)).build();
    }
    return NamenodeCommandProto.newBuilder().setAction(cmd.getAction()).build();
  }

  public static BlockKey[] convertBlockKeys(List<BlockKeyProto> list) {
    BlockKey[] ret = new BlockKey[list.size()];
    int i = 0;
    for (BlockKeyProto k : list) {
      ret[i++] = convert(k);
    }
    return ret;
  }

  public static NamespaceInfo convert(NamespaceInfoProto info) {
    StorageInfoProto storage = info.getStorageInfo();
    return new NamespaceInfo(storage.getNamespceID(), storage.getClusterID(),
        info.getBlockPoolID(), storage.getCTime(), info.getDistUpgradeVersion());
  }

  public static NamenodeCommand convert(NamenodeCommandProto cmd) {
    switch (cmd.getType()) {
    case CheckPointCommand:
      CheckpointCommandProto chkPt = cmd.getCheckpointCmd();
      return new CheckpointCommand(PBHelper.convert(chkPt.getSignature()),
          chkPt.getNeedToReturnImage());
    default:
      return new NamenodeCommand(cmd.getAction());
    }
  }

  public static ExtendedBlockProto convert(ExtendedBlock b) {
    return ExtendedBlockProto.newBuilder().setBlockId(b.getBlockId())
        .setGenerationStamp(b.getGenerationStamp())
        .setNumBytes(b.getNumBytes()).setPoolId(b.getBlockPoolId()).build();
  }

  public static ExtendedBlock convert(ExtendedBlockProto b) {
    return new ExtendedBlock(b.getPoolId(), b.getBlockId(), b.getNumBytes(),
        b.getGenerationStamp());
  }

  public static RecoveringBlockProto convert(RecoveringBlock b) {
    LocatedBlockProto lb = PBHelper.convert((LocatedBlock)b);
    return RecoveringBlockProto.newBuilder().setBlock(lb)
        .setNewGenStamp(b.getNewGenerationStamp()).build();
  }

  public static RecoveringBlock convert(RecoveringBlockProto b) {
    ExtendedBlock block = convert(b.getBlock().getB());
    DatanodeInfo[] locs = convert(b.getBlock().getLocsList());
    return new RecoveringBlock(block, locs, b.getNewGenStamp());
  }

  public static DatanodeInfo[] convert(List<DatanodeInfoProto> list) {
    DatanodeInfo[] info = new DatanodeInfo[list.size()];
    for (int i = 0; i < info.length; i++) {
      info[i] = convert(list.get(i));
    }
    return info;
  }

  public static DatanodeInfo convert(DatanodeInfoProto info) {
    DatanodeIDProto dnId = info.getId();
    return new DatanodeInfo(dnId.getName(), dnId.getStorageID(),
        dnId.getInfoPort(), dnId.getIpcPort(), info.getCapacity(),
        info.getDfsUsed(), info.getRemaining(), info.getBlockPoolUsed(),
        info.getLastUpdate(), info.getXceiverCount(), info.getLocation(),
        info.getHostName(), convert(info.getAdminState()));
  }
  
  public static DatanodeInfoProto convert(DatanodeInfo info) {
    return DatanodeInfoProto.newBuilder()
        .setAdminState(PBHelper.convert(info.getAdminState()))
        .setBlockPoolUsed(info.getBlockPoolUsed())
        .setCapacity(info.getCapacity())
        .setDfsUsed(info.getDfsUsed())
        .setHostName(info.getHostName())
        .setId(PBHelper.convert((DatanodeID)info))
        .setLastUpdate(info.getLastUpdate())
        .setLocation(info.getNetworkLocation())
        .setRemaining(info.getRemaining())
        .setXceiverCount(info.getXceiverCount())
        .build();
  }

  public static AdminStates convert(AdminState adminState) {
    switch(adminState) {
    case DECOMMISSION_INPROGRESS:
      return AdminStates.DECOMMISSION_INPROGRESS;
    case DECOMMISSIONED:
      return AdminStates.DECOMMISSIONED;
    case NORMAL:
    default:
      return AdminStates.NORMAL;
    }
  }
  
  public static AdminState convert(AdminStates adminState) {
    switch(adminState) {
    case DECOMMISSION_INPROGRESS:
      return AdminState.DECOMMISSION_INPROGRESS;
    case DECOMMISSIONED:
      return AdminState.DECOMMISSIONED;
    case NORMAL:
    default:
      return AdminState.NORMAL;
    }
  }

  public static LocatedBlockProto convert(LocatedBlock b) {
    Builder builder = LocatedBlockProto.newBuilder();
    DatanodeInfo[] locs = b.getLocations();
    for(DatanodeInfo loc : locs) {
      builder.addLocs(PBHelper.convert(loc));
    }
    return builder.setB(PBHelper.convert(b.getBlock()))
        .setBlockToken(PBHelper.convert(b.getBlockToken()))
        .setCorrupt(b.isCorrupt()).setOffset(b.getStartOffset()).build();
  }

  public static BlockTokenIdentifierProto convert(
      Token<BlockTokenIdentifier> token) {
    ByteString tokenId = ByteString.copyFrom(token.getIdentifier());
    ByteString password = ByteString.copyFrom(token.getPassword());
    return BlockTokenIdentifierProto.newBuilder().setIdentifier(tokenId)
        .setKind(token.getKind().toString()).setPassword(password)
        .setService(token.getService().toString()).build();
  }

  public static ReplicaState convert(ReplicaStateProto state) {
    switch (state) {
    case RBW:
      return ReplicaState.RBW;
    case RUR:
      return ReplicaState.RUR;
    case RWR:
      return ReplicaState.RWR;
    case TEMPORARY:
      return ReplicaState.TEMPORARY;
    case FINALIZED:
    default:
      return ReplicaState.FINALIZED;
    }
  }
}
