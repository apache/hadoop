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
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BalancerBandwidthCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockRecoveryCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.FinalizeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.KeyUpdateCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.UpgradeCommandProto;
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
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfosProto;
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
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

import com.google.protobuf.ByteString;

/**
 * Utilities for converting protobuf classes to and from implementation classes.
 */
public class PBHelper {
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
    return new Block(b.getBlockId(), b.getNumBytes(), b.getGenStamp());
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
    if (b == null) {
      return null;
    }
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
    DatanodeInfoProto.Builder builder = DatanodeInfoProto.newBuilder();
    builder.setBlockPoolUsed(info.getBlockPoolUsed());
    builder.setAdminState(PBHelper.convert(info.getAdminState()));
    builder.setCapacity(info.getCapacity())
        .setDfsUsed(info.getDfsUsed())
        .setHostName(info.getHostName())
        .setId(PBHelper.convert((DatanodeID)info))
        .setLastUpdate(info.getLastUpdate())
        .setLocation(info.getNetworkLocation())
        .setRemaining(info.getRemaining())
        .setXceiverCount(info.getXceiverCount())
        .build();
    return builder.build();
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
    if (b == null) {
      return null;
    }
    Builder builder = LocatedBlockProto.newBuilder();
    DatanodeInfo[] locs = b.getLocations();
    for (int i = 0; i < locs.length; i++) {
      builder.addLocs(i, PBHelper.convert(locs[i]));
    }
    return builder.setB(PBHelper.convert(b.getBlock()))
        .setBlockToken(PBHelper.convert(b.getBlockToken()))
        .setCorrupt(b.isCorrupt()).setOffset(b.getStartOffset()).build();
  }
  
  public static LocatedBlock convert(LocatedBlockProto proto) {
    List<DatanodeInfoProto> locs = proto.getLocsList();
    DatanodeInfo[] targets = new DatanodeInfo[locs.size()];
    for (int i = 0; i < locs.size(); i++) {
      targets[i] = PBHelper.convert(locs.get(i));
    }
    LocatedBlock lb = new LocatedBlock(PBHelper.convert(proto.getB()), targets,
        proto.getOffset(), proto.getCorrupt());
    lb.setBlockToken(PBHelper.convert(proto.getBlockToken()));
    return lb;
  }

  public static BlockTokenIdentifierProto convert(
      Token<BlockTokenIdentifier> token) {
    ByteString tokenId = ByteString.copyFrom(token.getIdentifier());
    ByteString password = ByteString.copyFrom(token.getPassword());
    return BlockTokenIdentifierProto.newBuilder().setIdentifier(tokenId)
        .setKind(token.getKind().toString()).setPassword(password)
        .setService(token.getService().toString()).build();
  }
  
  public static Token<BlockTokenIdentifier> convert(
      BlockTokenIdentifierProto blockToken) {
    return new Token<BlockTokenIdentifier>(blockToken.getIdentifier()
        .toByteArray(), blockToken.getPassword().toByteArray(), new Text(
        blockToken.getKind()), new Text(blockToken.getService()));
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

  public static ReplicaStateProto convert(ReplicaState state) {
    switch (state) {
    case RBW:
      return ReplicaStateProto.RBW;
    case RUR:
      return ReplicaStateProto.RUR;
    case RWR:
      return ReplicaStateProto.RWR;
    case TEMPORARY:
      return ReplicaStateProto.TEMPORARY;
    case FINALIZED:
    default:
      return ReplicaStateProto.FINALIZED;
    }
  }
  
  public static DatanodeRegistrationProto convert(
      DatanodeRegistration registration) {
    DatanodeRegistrationProto.Builder builder = DatanodeRegistrationProto
        .newBuilder();
    return builder.setDatanodeID(PBHelper.convert((DatanodeID) registration))
        .setStorageInfo(PBHelper.convert(registration.storageInfo))
        .setKeys(PBHelper.convert(registration.exportedKeys)).build();
  }

  public static DatanodeRegistration convert(DatanodeRegistrationProto proto) {
    return new DatanodeRegistration(PBHelper.convert(proto.getDatanodeID()),
        PBHelper.convert(proto.getStorageInfo()), PBHelper.convert(proto
            .getKeys()));
  }

  public static DatanodeCommand convert(DatanodeCommandProto proto) {
    switch (proto.getCmdType()) {
    case BalancerBandwidthCommand:
      return PBHelper.convert(proto.getBalancerCmd());
    case BlockCommand:
      return PBHelper.convert(proto.getBlkCmd());
    case BlockRecoveryCommand:
      return PBHelper.convert(proto.getRecoveryCmd());
    case FinalizeCommand:
      return PBHelper.convert(proto.getFinalizeCmd());
    case KeyUpdateCommand:
      return PBHelper.convert(proto.getKeyUpdateCmd());
    case RegisterCommand:
      return PBHelper.convert(proto.getRegisterCmd());
    case UpgradeCommand:
      return PBHelper.convert(proto.getUpgradeCmd());
    }
    return null;
  }
  
  public static BalancerBandwidthCommandProto convert(
      BalancerBandwidthCommand bbCmd) {
    return BalancerBandwidthCommandProto.newBuilder()
        .setBandwidth(bbCmd.getBalancerBandwidthValue()).build();
  }

  public static KeyUpdateCommandProto convert(KeyUpdateCommand cmd) {
    return KeyUpdateCommandProto.newBuilder()
        .setKeys(PBHelper.convert(cmd.getExportedKeys())).build();
  }

  public static BlockRecoveryCommandProto convert(BlockRecoveryCommand cmd) {
    BlockRecoveryCommandProto.Builder builder = BlockRecoveryCommandProto
        .newBuilder();
    for (RecoveringBlock b : cmd.getRecoveringBlocks()) {
      builder.addBlocks(PBHelper.convert(b));
    }
    return builder.build();
  }

  public static FinalizeCommandProto convert(FinalizeCommand cmd) {
    return FinalizeCommandProto.newBuilder()
        .setBlockPoolId(cmd.getBlockPoolId()).build();
  }
  
  public static RegisterCommandProto convert(RegisterCommand cmd) {
    return RegisterCommandProto.newBuilder().build();
  }

  public static BlockCommandProto convert(BlockCommand cmd) {
    BlockCommandProto.Builder builder = BlockCommandProto.newBuilder()
        .setBlockPoolId(cmd.getBlockPoolId());
    switch (cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      builder.setAction(BlockCommandProto.Action.TRANSFER);
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      builder.setAction(BlockCommandProto.Action.INVALIDATE);
      break;
    }
    Block[] blocks = cmd.getBlocks();
    for (int i = 0; i < blocks.length; i++) {
      builder.addBlocks(PBHelper.convert(blocks[i]));
    }
    DatanodeInfo[][] infos = cmd.getTargets();
    for (int i = 0; i < infos.length; i++) {
      builder.addTargets(PBHelper.convert(infos[i]));
    }
    return builder.build();
  }

  public static DatanodeInfosProto convert(DatanodeInfo[] datanodeInfos) {
    DatanodeInfosProto.Builder builder = DatanodeInfosProto.newBuilder();
    for (int i = 0; i < datanodeInfos.length; i++) {
      builder.addDatanodes(PBHelper.convert(datanodeInfos[i]));
    }
    return builder.build();
  }

  public static DatanodeCommandProto convert(DatanodeCommand datanodeCommand) {
    DatanodeCommandProto.Builder builder = DatanodeCommandProto.newBuilder();
    switch (datanodeCommand.getAction()) {
    case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
      builder.setCmdType(DatanodeCommandProto.Type.BalancerBandwidthCommand)
          .setBalancerCmd(
              PBHelper.convert((BalancerBandwidthCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
      builder
          .setCmdType(DatanodeCommandProto.Type.KeyUpdateCommand)
          .setKeyUpdateCmd(PBHelper.convert((KeyUpdateCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      builder.setCmdType(DatanodeCommandProto.Type.BlockRecoveryCommand)
          .setRecoveryCmd(
              PBHelper.convert((BlockRecoveryCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      builder.setCmdType(DatanodeCommandProto.Type.FinalizeCommand)
          .setFinalizeCmd(PBHelper.convert((FinalizeCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_REGISTER:
      builder.setCmdType(DatanodeCommandProto.Type.RegisterCommand)
          .setRegisterCmd(PBHelper.convert((RegisterCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_TRANSFER:
    case DatanodeProtocol.DNA_INVALIDATE:
      builder.setCmdType(DatanodeCommandProto.Type.BlockCommand).setBlkCmd(
          PBHelper.convert((BlockCommand) datanodeCommand));
      break;
    case DatanodeProtocol.DNA_SHUTDOWN: //Not expected
    case DatanodeProtocol.DNA_UNKNOWN: //Not expected
    }
    return builder.build();
  }

  public static UpgradeCommand convert(UpgradeCommandProto upgradeCmd) {
    int action = UpgradeCommand.UC_ACTION_UNKNOWN;
    switch (upgradeCmd.getAction()) {
    case REPORT_STATUS:
      action = UpgradeCommand.UC_ACTION_REPORT_STATUS;
      break;
    case START_UPGRADE:
      action = UpgradeCommand.UC_ACTION_START_UPGRADE;
    }
    return new UpgradeCommand(action, upgradeCmd.getVersion(),
        (short) upgradeCmd.getUpgradeStatus());
  }

  public static RegisterCommand convert(RegisterCommandProto registerCmd) {
    return new RegisterCommand();
  }

  public static KeyUpdateCommand convert(KeyUpdateCommandProto keyUpdateCmd) {
    return new KeyUpdateCommand(PBHelper.convert(keyUpdateCmd.getKeys()));
  }

  public static FinalizeCommand convert(FinalizeCommandProto finalizeCmd) {
    return new FinalizeCommand(finalizeCmd.getBlockPoolId());
  }

  public static BlockRecoveryCommand convert(
      BlockRecoveryCommandProto recoveryCmd) {
    List<RecoveringBlockProto> list = recoveryCmd.getBlocksList();
    List<RecoveringBlock> recoveringBlocks = new ArrayList<RecoveringBlock>(
        list.size());
    for (int i = 0; i < list.size(); i++) {
      recoveringBlocks.add(PBHelper.convert(list.get(0)));
    }
    return new BlockRecoveryCommand(recoveringBlocks);
  }

  public static BlockCommand convert(BlockCommandProto blkCmd) {
    List<BlockProto> blockProtoList = blkCmd.getBlocksList();
    List<DatanodeInfosProto> targetList = blkCmd.getTargetsList();
    DatanodeInfo[][] targets = new DatanodeInfo[blockProtoList.size()][];
    Block[] blocks = new Block[blockProtoList.size()];
    for (int i = 0; i < blockProtoList.size(); i++) {
      targets[i] = PBHelper.convert(targetList.get(i));
      blocks[i] = PBHelper.convert(blockProtoList.get(i));
    }
    int action = DatanodeProtocol.DNA_UNKNOWN;
    switch (blkCmd.getAction()) {
    case TRANSFER:
      action = DatanodeProtocol.DNA_TRANSFER;
      break;
    case INVALIDATE:
      action = DatanodeProtocol.DNA_INVALIDATE;
      break;
    }
    return new BlockCommand(action, blkCmd.getBlockPoolId(), blocks, targets);
  }

  public static DatanodeInfo[] convert(DatanodeInfosProto datanodeInfosProto) {
    List<DatanodeInfoProto> proto = datanodeInfosProto.getDatanodesList();
    DatanodeInfo[] infos = new DatanodeInfo[proto.size()];
    for (int i = 0; i < infos.length; i++) {
      infos[i] = PBHelper.convert(proto.get(i));
    }
    return infos;
  }

  public static BalancerBandwidthCommand convert(
      BalancerBandwidthCommandProto balancerCmd) {
    return new BalancerBandwidthCommand(balancerCmd.getBandwidth());
  }

  public static ReceivedDeletedBlockInfoProto convert(
      ReceivedDeletedBlockInfo receivedDeletedBlockInfo) {
    return ReceivedDeletedBlockInfoProto.newBuilder()
        .setBlock(PBHelper.convert(receivedDeletedBlockInfo.getBlock()))
        .setDeleteHint(receivedDeletedBlockInfo.getDelHints()).build();
  }

  public static UpgradeCommandProto convert(UpgradeCommand comm) {
    UpgradeCommandProto.Builder builder = UpgradeCommandProto.newBuilder()
        .setVersion(comm.getVersion())
        .setUpgradeStatus(comm.getCurrentStatus());
    switch (comm.getAction()) {
    case UpgradeCommand.UC_ACTION_REPORT_STATUS:
      builder.setAction(UpgradeCommandProto.Action.REPORT_STATUS);
      break;
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      builder.setAction(UpgradeCommandProto.Action.START_UPGRADE);
      break;
    default:
      builder.setAction(UpgradeCommandProto.Action.UNKNOWN);
      break;
    }
    return builder.build();
  }

  public static ReceivedDeletedBlockInfo convert(
      ReceivedDeletedBlockInfoProto proto) {
    return new ReceivedDeletedBlockInfo(PBHelper.convert(proto.getBlock()),
        proto.getDeleteHint());
  }
  
  public static NamespaceInfoProto convert(NamespaceInfo info) {
    return NamespaceInfoProto.newBuilder()
        .setBlockPoolID(info.getBlockPoolID())
        .setBuildVersion(info.getBuildVersion())
        .setDistUpgradeVersion(info.getDistributedUpgradeVersion())
        .setStorageInfo(PBHelper.convert((StorageInfo)info)).build();
  }
}
