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


import com.google.protobuf.UninitializedMessageException;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockECReconstructionCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockRecoveryCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeStorageProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.BlockKeyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.BlockWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.BlocksWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.CheckpointSignatureProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.ExportedBlockKeysProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamenodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamenodeRegistrationProto.NamenodeRoleProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamespaceInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.RecoveringBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.RemoteEditLogManifestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand.BlockECReconstructionInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.StripedBlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * Tests for {@link PBHelper}
 */
public class TestPBHelper {

  /**
   * Used for asserting equality on doubles.
   */
  private static final double DELTA = 0.000001;

  @Test
  public void testGetByteString() {
    assertSame(ByteString.EMPTY, PBHelperClient.getByteString(new byte[0]));
  }

  @Test
  public void testConvertNamenodeRole() {
    assertEquals(NamenodeRoleProto.BACKUP,
        PBHelper.convert(NamenodeRole.BACKUP));
    assertEquals(NamenodeRoleProto.CHECKPOINT,
        PBHelper.convert(NamenodeRole.CHECKPOINT));
    assertEquals(NamenodeRoleProto.NAMENODE,
        PBHelper.convert(NamenodeRole.NAMENODE));
    assertEquals(NamenodeRole.BACKUP,
        PBHelper.convert(NamenodeRoleProto.BACKUP));
    assertEquals(NamenodeRole.CHECKPOINT,
        PBHelper.convert(NamenodeRoleProto.CHECKPOINT));
    assertEquals(NamenodeRole.NAMENODE,
        PBHelper.convert(NamenodeRoleProto.NAMENODE));
  }

  private static StorageInfo getStorageInfo(NodeType type) {
    return new StorageInfo(1, 2, "cid", 3, type);
  }

  @Test
  public void testConvertStoragInfo() {
    StorageInfo info = getStorageInfo(NodeType.NAME_NODE);
    StorageInfoProto infoProto = PBHelper.convert(info);
    StorageInfo info2 = PBHelper.convert(infoProto, NodeType.NAME_NODE);
    assertEquals(info.getClusterID(), info2.getClusterID());
    assertEquals(info.getCTime(), info2.getCTime());
    assertEquals(info.getLayoutVersion(), info2.getLayoutVersion());
    assertEquals(info.getNamespaceID(), info2.getNamespaceID());
  }

  @Test
  public void testConvertNamenodeRegistration() {
    StorageInfo info = getStorageInfo(NodeType.NAME_NODE);
    NamenodeRegistration reg = new NamenodeRegistration("address:999",
        "http:1000", info, NamenodeRole.NAMENODE);
    NamenodeRegistrationProto regProto = PBHelper.convert(reg);
    NamenodeRegistration reg2 = PBHelper.convert(regProto);
    assertEquals(reg.getAddress(), reg2.getAddress());
    assertEquals(reg.getClusterID(), reg2.getClusterID());
    assertEquals(reg.getCTime(), reg2.getCTime());
    assertEquals(reg.getHttpAddress(), reg2.getHttpAddress());
    assertEquals(reg.getLayoutVersion(), reg2.getLayoutVersion());
    assertEquals(reg.getNamespaceID(), reg2.getNamespaceID());
    assertEquals(reg.getRegistrationID(), reg2.getRegistrationID());
    assertEquals(reg.getRole(), reg2.getRole());
    assertEquals(reg.getVersion(), reg2.getVersion());

  }

  @Test
  public void testConvertDatanodeID() {
    DatanodeID dn = DFSTestUtil.getLocalDatanodeID();
    DatanodeIDProto dnProto = PBHelperClient.convert(dn);
    DatanodeID dn2 = PBHelperClient.convert(dnProto);
    compare(dn, dn2);
  }
  
  void compare(DatanodeID dn, DatanodeID dn2) {
    assertEquals(dn.getIpAddr(), dn2.getIpAddr());
    assertEquals(dn.getHostName(), dn2.getHostName());
    assertEquals(dn.getDatanodeUuid(), dn2.getDatanodeUuid());
    assertEquals(dn.getXferPort(), dn2.getXferPort());
    assertEquals(dn.getInfoPort(), dn2.getInfoPort());
    assertEquals(dn.getIpcPort(), dn2.getIpcPort());
  }

  void compare(DatanodeStorage dns1, DatanodeStorage dns2) {
    assertThat(dns2.getStorageID(), is(dns1.getStorageID()));
    assertThat(dns2.getState(), is(dns1.getState()));
    assertThat(dns2.getStorageType(), is(dns1.getStorageType()));
  }

  @Test
  public void testConvertBlock() {
    Block b = new Block(1, 100, 3);
    BlockProto bProto = PBHelperClient.convert(b);
    Block b2 = PBHelperClient.convert(bProto);
    assertEquals(b, b2);
  }

  @Test
  public void testConvertBlockType() {
    BlockType bContiguous = BlockType.CONTIGUOUS;
    BlockTypeProto bContiguousProto = PBHelperClient.convert(bContiguous);
    BlockType bContiguous2 = PBHelperClient.convert(bContiguousProto);
    assertEquals(bContiguous, bContiguous2);

    BlockType bStriped = BlockType.STRIPED;
    BlockTypeProto bStripedProto = PBHelperClient.convert(bStriped);
    BlockType bStriped2 = PBHelperClient.convert(bStripedProto);
    assertEquals(bStriped, bStriped2);
  }

  private static BlockWithLocations getBlockWithLocations(
      int bid, boolean isStriped) {
    final String[] datanodeUuids = {"dn1", "dn2", "dn3"};
    final String[] storageIDs = {"s1", "s2", "s3"};
    final StorageType[] storageTypes = {
        StorageType.DISK, StorageType.DISK, StorageType.DISK};
    final byte[] indices = {0, 1, 2};
    final short dataBlkNum = 6;
    BlockWithLocations blkLocs = new BlockWithLocations(new Block(bid, 0, 1),
        datanodeUuids, storageIDs, storageTypes);
    if (isStriped) {
      blkLocs = new StripedBlockWithLocations(blkLocs, indices, dataBlkNum,
          StripedFileTestUtil.getDefaultECPolicy().getCellSize());
    }
    return blkLocs;
  }

  private void compare(BlockWithLocations locs1, BlockWithLocations locs2) {
    assertEquals(locs1.getBlock(), locs2.getBlock());
    assertTrue(Arrays.equals(locs1.getStorageIDs(), locs2.getStorageIDs()));
    if (locs1 instanceof StripedBlockWithLocations) {
      assertTrue(Arrays.equals(((StripedBlockWithLocations) locs1).getIndices(),
          ((StripedBlockWithLocations) locs2).getIndices()));
    }
  }

  @Test
  public void testConvertBlockWithLocations() {
    boolean[] testSuite = new boolean[]{false, true};
    for (int i = 0; i < testSuite.length; i++) {
      BlockWithLocations locs = getBlockWithLocations(1, testSuite[i]);
      BlockWithLocationsProto locsProto = PBHelper.convert(locs);
      BlockWithLocations locs2 = PBHelper.convert(locsProto);
      compare(locs, locs2);
    }
  }

  @Test
  public void testConvertBlocksWithLocations() {
    boolean[] testSuite = new boolean[]{false, true};
    for (int i = 0; i < testSuite.length; i++) {
      BlockWithLocations[] list = new BlockWithLocations[]{
          getBlockWithLocations(1, testSuite[i]),
          getBlockWithLocations(2, testSuite[i])};
      BlocksWithLocations locs = new BlocksWithLocations(list);
      BlocksWithLocationsProto locsProto = PBHelper.convert(locs);
      BlocksWithLocations locs2 = PBHelper.convert(locsProto);
      BlockWithLocations[] blocks = locs.getBlocks();
      BlockWithLocations[] blocks2 = locs2.getBlocks();
      assertEquals(blocks.length, blocks2.length);
      for (int j = 0; j < blocks.length; j++) {
        compare(blocks[j], blocks2[j]);
      }
    }
  }

  private static BlockKey getBlockKey(int keyId) {
    return new BlockKey(keyId, 10, "encodedKey".getBytes());
  }

  private void compare(BlockKey k1, BlockKey k2) {
    assertEquals(k1.getExpiryDate(), k2.getExpiryDate());
    assertEquals(k1.getKeyId(), k2.getKeyId());
    assertTrue(Arrays.equals(k1.getEncodedKey(), k2.getEncodedKey()));
  }

  @Test
  public void testConvertBlockKey() {
    BlockKey key = getBlockKey(1);
    BlockKeyProto keyProto = PBHelper.convert(key);
    BlockKey key1 = PBHelper.convert(keyProto);
    compare(key, key1);
  }

  @Test
  public void testConvertExportedBlockKeys() {
    BlockKey[] keys = new BlockKey[] { getBlockKey(2), getBlockKey(3) };
    ExportedBlockKeys expKeys = new ExportedBlockKeys(true, 9, 10,
        getBlockKey(1), keys);
    ExportedBlockKeysProto expKeysProto = PBHelper.convert(expKeys);
    ExportedBlockKeys expKeys1 = PBHelper.convert(expKeysProto);
    compare(expKeys, expKeys1);
  }
  
  void compare(ExportedBlockKeys expKeys, ExportedBlockKeys expKeys1) {
    BlockKey[] allKeys = expKeys.getAllKeys();
    BlockKey[] allKeys1 = expKeys1.getAllKeys();
    assertEquals(allKeys.length, allKeys1.length);
    for (int i = 0; i < allKeys.length; i++) {
      compare(allKeys[i], allKeys1[i]);
    }
    compare(expKeys.getCurrentKey(), expKeys1.getCurrentKey());
    assertEquals(expKeys.getKeyUpdateInterval(),
        expKeys1.getKeyUpdateInterval());
    assertEquals(expKeys.getTokenLifetime(), expKeys1.getTokenLifetime());
  }

  @Test
  public void testConvertCheckpointSignature() {
    CheckpointSignature s = new CheckpointSignature(
        getStorageInfo(NodeType.NAME_NODE), "bpid", 100, 1);
    CheckpointSignatureProto sProto = PBHelper.convert(s);
    CheckpointSignature s1 = PBHelper.convert(sProto);
    assertEquals(s.getBlockpoolID(), s1.getBlockpoolID());
    assertEquals(s.getClusterID(), s1.getClusterID());
    assertEquals(s.getCTime(), s1.getCTime());
    assertEquals(s.getCurSegmentTxId(), s1.getCurSegmentTxId());
    assertEquals(s.getLayoutVersion(), s1.getLayoutVersion());
    assertEquals(s.getMostRecentCheckpointTxId(),
        s1.getMostRecentCheckpointTxId());
    assertEquals(s.getNamespaceID(), s1.getNamespaceID());
  }
  
  private static void compare(RemoteEditLog l1, RemoteEditLog l2) {
    assertEquals(l1.getEndTxId(), l2.getEndTxId());
    assertEquals(l1.getStartTxId(), l2.getStartTxId());
  }
  
  @Test
  public void testConvertRemoteEditLog() {
    RemoteEditLog l = new RemoteEditLog(1, 100);
    RemoteEditLogProto lProto = PBHelper.convert(l);
    RemoteEditLog l1 = PBHelper.convert(lProto);
    compare(l, l1);
  }

  private void convertAndCheckRemoteEditLogManifest(RemoteEditLogManifest m,
                                                    List<RemoteEditLog> logs,
                                                    long committedTxnId) {
    RemoteEditLogManifestProto mProto = PBHelper.convert(m);
    RemoteEditLogManifest m1 = PBHelper.convert(mProto);

    List<RemoteEditLog> logs1 = m1.getLogs();
    assertEquals(logs.size(), logs1.size());
    for (int i = 0; i < logs.size(); i++) {
      compare(logs.get(i), logs1.get(i));
    }
    assertEquals(committedTxnId, m.getCommittedTxnId());
  }

  @Test
  public void testConvertRemoteEditLogManifest() {
    List<RemoteEditLog> logs = new ArrayList<RemoteEditLog>();
    logs.add(new RemoteEditLog(1, 10));
    logs.add(new RemoteEditLog(11, 20));

    convertAndCheckRemoteEditLogManifest(new RemoteEditLogManifest(logs, 20),
        logs, 20);
    convertAndCheckRemoteEditLogManifest(new RemoteEditLogManifest(logs),
        logs, HdfsServerConstants.INVALID_TXID);
  }

  public ExtendedBlock getExtendedBlock() {
    return getExtendedBlock(1);
  }

  public ExtendedBlock getExtendedBlock(long blkid) {
    return new ExtendedBlock("bpid", blkid, 100, 2);
  }
  
  private void compare(DatanodeInfo dn1, DatanodeInfo dn2) {
      assertEquals(dn1.getAdminState(), dn2.getAdminState());
      assertEquals(dn1.getBlockPoolUsed(), dn2.getBlockPoolUsed());
      assertEquals(dn1.getBlockPoolUsedPercent(), 
          dn2.getBlockPoolUsedPercent(), DELTA);
      assertEquals(dn1.getCapacity(), dn2.getCapacity());
      assertEquals(dn1.getDatanodeReport(), dn2.getDatanodeReport());
      assertEquals(dn1.getDfsUsed(), dn1.getDfsUsed());
      assertEquals(dn1.getDfsUsedPercent(), dn1.getDfsUsedPercent(), DELTA);
      assertEquals(dn1.getIpAddr(), dn2.getIpAddr());
      assertEquals(dn1.getHostName(), dn2.getHostName());
      assertEquals(dn1.getInfoPort(), dn2.getInfoPort());
      assertEquals(dn1.getIpcPort(), dn2.getIpcPort());
      assertEquals(dn1.getLastUpdate(), dn2.getLastUpdate());
      assertEquals(dn1.getLevel(), dn2.getLevel());
      assertEquals(dn1.getNetworkLocation(), dn2.getNetworkLocation());
  }
  
  @Test
  public void testConvertExtendedBlock() {
    ExtendedBlock b = getExtendedBlock();
    ExtendedBlockProto bProto = PBHelperClient.convert(b);
    ExtendedBlock b1 = PBHelperClient.convert(bProto);
    assertEquals(b, b1);
    
    b.setBlockId(-1);
    bProto = PBHelperClient.convert(b);
    b1 = PBHelperClient.convert(bProto);
    assertEquals(b, b1);
  }
  
  @Test
  public void testConvertRecoveringBlock() {
    DatanodeInfo di1 = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo di2 = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo[] dnInfo = new DatanodeInfo[] { di1, di2 };
    RecoveringBlock b = new RecoveringBlock(getExtendedBlock(), dnInfo, 3);
    RecoveringBlockProto bProto = PBHelper.convert(b);
    RecoveringBlock b1 = PBHelper.convert(bProto);
    assertEquals(b.getBlock(), b1.getBlock());
    DatanodeInfo[] dnInfo1 = b1.getLocations();
    assertEquals(dnInfo.length, dnInfo1.length);
    for (int i=0; i < dnInfo.length; i++) {
      compare(dnInfo[0], dnInfo1[0]);
    }
  }
  
  @Test
  public void testConvertBlockRecoveryCommand() {
    DatanodeInfo di1 = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo di2 = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo[] dnInfo = new DatanodeInfo[] { di1, di2 };

    List<RecoveringBlock> blks = ImmutableList.of(
      new RecoveringBlock(getExtendedBlock(1), dnInfo, 3),
      new RecoveringBlock(getExtendedBlock(2), dnInfo, 3)
    );
    
    BlockRecoveryCommand cmd = new BlockRecoveryCommand(blks);
    BlockRecoveryCommandProto proto = PBHelper.convert(cmd);
    assertEquals(1, proto.getBlocks(0).getBlock().getB().getBlockId());
    assertEquals(2, proto.getBlocks(1).getBlock().getB().getBlockId());
    
    BlockRecoveryCommand cmd2 = PBHelper.convert(proto);
    
    List<RecoveringBlock> cmd2Blks = Lists.newArrayList(
        cmd2.getRecoveringBlocks());
    assertEquals(blks.get(0).getBlock(), cmd2Blks.get(0).getBlock());
    assertEquals(blks.get(1).getBlock(), cmd2Blks.get(1).getBlock());
    assertEquals(Joiner.on(",").join(blks), Joiner.on(",").join(cmd2Blks));
    assertEquals(cmd.toString(), cmd2.toString());
  }
  
  
  @Test
  public void testConvertText() {
    Text t = new Text("abc".getBytes());
    String s = t.toString();
    Text t1 = new Text(s);
    assertEquals(t, t1);
  }
  
  @Test
  public void testConvertBlockToken() {
    Token<BlockTokenIdentifier> token = new Token<BlockTokenIdentifier>(
        "identifier".getBytes(), "password".getBytes(), new Text("kind"),
        new Text("service"));
    TokenProto tokenProto = PBHelperClient.convert(token);
    Token<BlockTokenIdentifier> token2 = PBHelperClient.convert(tokenProto);
    compare(token, token2);
  }
  
  @Test
  public void testConvertNamespaceInfo() {
    NamespaceInfo info = new NamespaceInfo(37, "clusterID", "bpID", 2300);
    NamespaceInfoProto proto = PBHelper.convert(info);
    NamespaceInfo info2 = PBHelper.convert(proto);
    compare(info, info2); //Compare the StorageInfo
    assertEquals(info.getBlockPoolID(), info2.getBlockPoolID());
    assertEquals(info.getBuildVersion(), info2.getBuildVersion());
  }

  private void compare(StorageInfo expected, StorageInfo actual) {
    assertEquals(expected.clusterID, actual.clusterID);
    assertEquals(expected.namespaceID, actual.namespaceID);
    assertEquals(expected.cTime, actual.cTime);
    assertEquals(expected.layoutVersion, actual.layoutVersion);
  }

  private void compare(Token<BlockTokenIdentifier> expected,
      Token<BlockTokenIdentifier> actual) {
    assertTrue(Arrays.equals(expected.getIdentifier(), actual.getIdentifier()));
    assertTrue(Arrays.equals(expected.getPassword(), actual.getPassword()));
    assertEquals(expected.getKind(), actual.getKind());
    assertEquals(expected.getService(), actual.getService());
  }

  private void compare(LocatedBlock expected, LocatedBlock actual) {
    assertEquals(expected.getBlock(), actual.getBlock());
    compare(expected.getBlockToken(), actual.getBlockToken());
    assertEquals(expected.getStartOffset(), actual.getStartOffset());
    assertEquals(expected.isCorrupt(), actual.isCorrupt());
    DatanodeInfo [] ei = expected.getLocations();
    DatanodeInfo [] ai = actual.getLocations();
    assertEquals(ei.length, ai.length);
    for (int i = 0; i < ei.length ; i++) {
      compare(ei[i], ai[i]);
    }
  }

  private LocatedBlock createLocatedBlock() {
    DatanodeInfo[] dnInfos = {
        DFSTestUtil.getLocalDatanodeInfo("127.0.0.1", "h1",
            AdminStates.DECOMMISSION_INPROGRESS),
        DFSTestUtil.getLocalDatanodeInfo("127.0.0.1", "h2",
            AdminStates.DECOMMISSIONED),
        DFSTestUtil.getLocalDatanodeInfo("127.0.0.1", "h3", 
            AdminStates.NORMAL),
        DFSTestUtil.getLocalDatanodeInfo("127.0.0.1", "h4",
            AdminStates.NORMAL),
    };
    String[] storageIDs = {"s1", "s2", "s3", "s4"};
    StorageType[] media = {
        StorageType.DISK,
        StorageType.SSD,
        StorageType.DISK,
        StorageType.RAM_DISK
    };
    LocatedBlock lb = new LocatedBlock(
        new ExtendedBlock("bp12", 12345, 10, 53),
        dnInfos, storageIDs, media, 5, false, new DatanodeInfo[]{});
    lb.setBlockToken(new Token<BlockTokenIdentifier>(
        "identifier".getBytes(), "password".getBytes(), new Text("kind"),
        new Text("service")));
    return lb;
  }

  private LocatedBlock createLocatedBlockNoStorageMedia() {
    DatanodeInfo[] dnInfos = {
        DFSTestUtil.getLocalDatanodeInfo("127.0.0.1", "h1",
                                         AdminStates.DECOMMISSION_INPROGRESS),
        DFSTestUtil.getLocalDatanodeInfo("127.0.0.1", "h2",
                                         AdminStates.DECOMMISSIONED),
        DFSTestUtil.getLocalDatanodeInfo("127.0.0.1", "h3",
                                         AdminStates.NORMAL)
    };
    LocatedBlock lb = new LocatedBlock(
        new ExtendedBlock("bp12", 12345, 10, 53), dnInfos);
    lb.setBlockToken(new Token<BlockTokenIdentifier>(
        "identifier".getBytes(), "password".getBytes(), new Text("kind"),
        new Text("service")));
    lb.setStartOffset(5);
    return lb;
  }

  @Test
  public void testConvertLocatedBlock() {
    LocatedBlock lb = createLocatedBlock();
    LocatedBlockProto lbProto = PBHelperClient.convertLocatedBlock(lb);
    LocatedBlock lb2 = PBHelperClient.convertLocatedBlockProto(lbProto);
    compare(lb,lb2);
  }

  @Test
  public void testConvertLocatedBlockNoStorageMedia() {
    LocatedBlock lb = createLocatedBlockNoStorageMedia();
    LocatedBlockProto lbProto = PBHelperClient.convertLocatedBlock(lb);
    LocatedBlock lb2 = PBHelperClient.convertLocatedBlockProto(lbProto);
    compare(lb,lb2);
  }

  @Test
  public void testConvertLocatedBlockList() {
    ArrayList<LocatedBlock> lbl = new ArrayList<LocatedBlock>();
    for (int i=0;i<3;i++) {
      lbl.add(createLocatedBlock());
    }
    List<LocatedBlockProto> lbpl = PBHelperClient.convertLocatedBlocks2(lbl);
    List<LocatedBlock> lbl2 = PBHelperClient.convertLocatedBlocks(lbpl);
    assertEquals(lbl.size(), lbl2.size());
    for (int i=0;i<lbl.size();i++) {
      compare(lbl.get(i), lbl2.get(2));
    }
  }
  
  @Test
  public void testConvertLocatedBlockArray() {
    LocatedBlock [] lbl = new LocatedBlock[3];
    for (int i=0;i<3;i++) {
      lbl[i] = createLocatedBlock();
    }
    LocatedBlockProto [] lbpl = PBHelperClient.convertLocatedBlocks(lbl);
    LocatedBlock [] lbl2 = PBHelperClient.convertLocatedBlocks(lbpl);
    assertEquals(lbl.length, lbl2.length);
    for (int i=0;i<lbl.length;i++) {
      compare(lbl[i], lbl2[i]);
    }
  }

  @Test
  public void testConvertDatanodeRegistration() {
    DatanodeID dnId = DFSTestUtil.getLocalDatanodeID();
    BlockKey[] keys = new BlockKey[] { getBlockKey(2), getBlockKey(3) };
    ExportedBlockKeys expKeys = new ExportedBlockKeys(true, 9, 10,
        getBlockKey(1), keys);
    DatanodeRegistration reg = new DatanodeRegistration(dnId,
        new StorageInfo(NodeType.DATA_NODE), expKeys, "3.0.0");
    DatanodeRegistrationProto proto = PBHelper.convert(reg);
    DatanodeRegistration reg2 = PBHelper.convert(proto);
    compare(reg.getStorageInfo(), reg2.getStorageInfo());
    compare(reg.getExportedKeys(), reg2.getExportedKeys());
    compare(reg, reg2);
    assertEquals(reg.getSoftwareVersion(), reg2.getSoftwareVersion());
  }

  @Test
  public void TestConvertDatanodeStorage() {
    DatanodeStorage dns1 = new DatanodeStorage(
        "id1", DatanodeStorage.State.NORMAL, StorageType.SSD);

    DatanodeStorageProto proto = PBHelperClient.convert(dns1);
    DatanodeStorage dns2 = PBHelperClient.convert(proto);
    compare(dns1, dns2);
  }
  
  @Test
  public void testConvertBlockCommand() {
    Block[] blocks = new Block[] { new Block(21), new Block(22) };
    DatanodeInfo[][] dnInfos = new DatanodeInfo[][] { new DatanodeInfo[1],
        new DatanodeInfo[2] };
    dnInfos[0][0] = DFSTestUtil.getLocalDatanodeInfo();
    dnInfos[1][0] = DFSTestUtil.getLocalDatanodeInfo();
    dnInfos[1][1] = DFSTestUtil.getLocalDatanodeInfo();
    String[][] storageIDs = {{"s00"}, {"s10", "s11"}};
    StorageType[][] storageTypes = {{StorageType.DEFAULT},
        {StorageType.DEFAULT, StorageType.DEFAULT}};
    BlockCommand bc = new BlockCommand(DatanodeProtocol.DNA_TRANSFER, "bp1",
        blocks, dnInfos, storageTypes, storageIDs);
    BlockCommandProto bcProto = PBHelper.convert(bc);
    BlockCommand bc2 = PBHelper.convert(bcProto);
    assertEquals(bc.getAction(), bc2.getAction());
    assertEquals(bc.getBlocks().length, bc2.getBlocks().length);
    Block[] blocks2 = bc2.getBlocks();
    for (int i = 0; i < blocks.length; i++) {
      assertEquals(blocks[i], blocks2[i]);
    }
    DatanodeInfo[][] dnInfos2 = bc2.getTargets();
    assertEquals(dnInfos.length, dnInfos2.length);
    for (int i = 0; i < dnInfos.length; i++) {
      DatanodeInfo[] d1 = dnInfos[i];
      DatanodeInfo[] d2 = dnInfos2[i];
      assertEquals(d1.length, d2.length);
      for (int j = 0; j < d1.length; j++) {
        compare(d1[j], d2[j]);
      }
    }
  }
  
  @Test
  public void testChecksumTypeProto() {
    assertEquals(DataChecksum.Type.NULL,
        PBHelperClient.convert(HdfsProtos.ChecksumTypeProto.CHECKSUM_NULL));
    assertEquals(DataChecksum.Type.CRC32,
        PBHelperClient.convert(HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32));
    assertEquals(DataChecksum.Type.CRC32C,
        PBHelperClient.convert(HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C));
    assertEquals(PBHelperClient.convert(DataChecksum.Type.NULL),
        HdfsProtos.ChecksumTypeProto.CHECKSUM_NULL);
    assertEquals(PBHelperClient.convert(DataChecksum.Type.CRC32),
        HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32);
    assertEquals(PBHelperClient.convert(DataChecksum.Type.CRC32C),
        HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C);
  }

  @Test
  public void testAclEntryProto() {
    // All fields populated.
    AclEntry e1 = new AclEntry.Builder().setName("test")
        .setPermission(FsAction.READ_EXECUTE).setScope(AclEntryScope.DEFAULT)
        .setType(AclEntryType.OTHER).build();
    // No name.
    AclEntry e2 = new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.USER).setPermission(FsAction.ALL).build();
    // No permission, which will default to the 0'th enum element.
    AclEntry e3 = new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.USER).setName("test").build();
    AclEntry[] expected = new AclEntry[] { e1, e2,
        new AclEntry.Builder()
            .setScope(e3.getScope())
            .setType(e3.getType())
            .setName(e3.getName())
            .setPermission(FsAction.NONE)
            .build() };
    AclEntry[] actual = Lists.newArrayList(
        PBHelperClient.convertAclEntry(PBHelperClient.convertAclEntryProto(Lists
            .newArrayList(e1, e2, e3)))).toArray(new AclEntry[0]);
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testAclStatusProto() {
    AclEntry e = new AclEntry.Builder().setName("test")
        .setPermission(FsAction.READ_EXECUTE).setScope(AclEntryScope.DEFAULT)
        .setType(AclEntryType.OTHER).build();
    AclStatus s = new AclStatus.Builder().owner("foo").group("bar").addEntry(e)
        .build();
    Assert.assertEquals(s, PBHelperClient.convert(PBHelperClient.convert(s)));
  }
  
  @Test
  public void testBlockECRecoveryCommand() {
    DatanodeInfo[] dnInfos0 = new DatanodeInfo[] {
        DFSTestUtil.getLocalDatanodeInfo(), DFSTestUtil.getLocalDatanodeInfo() };
    DatanodeStorageInfo targetDnInfos_0 = BlockManagerTestUtil
        .newDatanodeStorageInfo(DFSTestUtil.getLocalDatanodeDescriptor(),
            new DatanodeStorage("s00"));
    DatanodeStorageInfo targetDnInfos_1 = BlockManagerTestUtil
        .newDatanodeStorageInfo(DFSTestUtil.getLocalDatanodeDescriptor(),
            new DatanodeStorage("s01"));
    DatanodeStorageInfo[] targetDnInfos0 = new DatanodeStorageInfo[] {
        targetDnInfos_0, targetDnInfos_1 };
    byte[] liveBlkIndices0 = new byte[2];
    BlockECReconstructionInfo blkECRecoveryInfo0 = new BlockECReconstructionInfo(
        new ExtendedBlock("bp1", 1234), dnInfos0, targetDnInfos0,
        liveBlkIndices0, StripedFileTestUtil.getDefaultECPolicy());
    DatanodeInfo[] dnInfos1 = new DatanodeInfo[] {
        DFSTestUtil.getLocalDatanodeInfo(), DFSTestUtil.getLocalDatanodeInfo() };
    DatanodeStorageInfo targetDnInfos_2 = BlockManagerTestUtil
        .newDatanodeStorageInfo(DFSTestUtil.getLocalDatanodeDescriptor(),
            new DatanodeStorage("s02"));
    DatanodeStorageInfo targetDnInfos_3 = BlockManagerTestUtil
        .newDatanodeStorageInfo(DFSTestUtil.getLocalDatanodeDescriptor(),
            new DatanodeStorage("s03"));
    DatanodeStorageInfo[] targetDnInfos1 = new DatanodeStorageInfo[] {
        targetDnInfos_2, targetDnInfos_3 };
    byte[] liveBlkIndices1 = new byte[2];
    BlockECReconstructionInfo blkECRecoveryInfo1 = new BlockECReconstructionInfo(
        new ExtendedBlock("bp2", 3256), dnInfos1, targetDnInfos1,
        liveBlkIndices1, StripedFileTestUtil.getDefaultECPolicy());
    List<BlockECReconstructionInfo> blkRecoveryInfosList = new ArrayList<BlockECReconstructionInfo>();
    blkRecoveryInfosList.add(blkECRecoveryInfo0);
    blkRecoveryInfosList.add(blkECRecoveryInfo1);
    BlockECReconstructionCommand blkECReconstructionCmd = new BlockECReconstructionCommand(
        DatanodeProtocol.DNA_ERASURE_CODING_RECONSTRUCTION, blkRecoveryInfosList);
    BlockECReconstructionCommandProto blkECRecoveryCmdProto = PBHelper
        .convert(blkECReconstructionCmd);
    blkECReconstructionCmd = PBHelper.convert(blkECRecoveryCmdProto);
    Iterator<BlockECReconstructionInfo> iterator = blkECReconstructionCmd.getECTasks()
        .iterator();
    assertBlockECRecoveryInfoEquals(blkECRecoveryInfo0, iterator.next());
    assertBlockECRecoveryInfoEquals(blkECRecoveryInfo1, iterator.next());
  }

  @Test
  public void testDataNodeInfoPBHelper() {
    DatanodeID id = DFSTestUtil.getLocalDatanodeID();
    DatanodeInfo dnInfos0 = new DatanodeInfoBuilder().setNodeID(id)
        .build();
    dnInfos0.setCapacity(3500L);
    dnInfos0.setDfsUsed(1000L);
    dnInfos0.setNonDfsUsed(2000L);
    dnInfos0.setRemaining(500L);
    HdfsProtos.DatanodeInfoProto dnproto = PBHelperClient.convert(dnInfos0);
    DatanodeInfo dnInfos1 = PBHelperClient.convert(dnproto);
    compare(dnInfos0, dnInfos1);
    assertEquals(dnInfos0.getNonDfsUsed(), dnInfos1.getNonDfsUsed());

    //Testing without nonDfs field
    HdfsProtos.DatanodeInfoProto.Builder b =
        HdfsProtos.DatanodeInfoProto.newBuilder();
    b.setId(PBHelperClient.convert(id)).setCapacity(3500L).setDfsUsed(1000L)
        .setRemaining(500L);
    DatanodeInfo dnInfos3 = PBHelperClient.convert(b.build());
    assertEquals(dnInfos0.getNonDfsUsed(), dnInfos3.getNonDfsUsed());
  }

  @Test
  public void testSlowPeerInfoPBHelper() {
    // Test with a map that has a few slow peer entries.
    final SlowPeerReports slowPeers = SlowPeerReports.create(
        ImmutableMap.of("peer1", 0.0, "peer2", 1.0, "peer3", 2.0));
    SlowPeerReports slowPeersConverted1 = PBHelper.convertSlowPeerInfo(
        PBHelper.convertSlowPeerInfo(slowPeers));
    assertTrue(
        "Expected map:" + slowPeers + ", got map:" +
            slowPeersConverted1.getSlowPeers(),
        slowPeersConverted1.equals(slowPeers));

    // Test with an empty map.
    SlowPeerReports slowPeersConverted2 = PBHelper.convertSlowPeerInfo(
        PBHelper.convertSlowPeerInfo(SlowPeerReports.EMPTY_REPORT));
    assertTrue(
        "Expected empty map:" + ", got map:" + slowPeersConverted2,
        slowPeersConverted2.equals(SlowPeerReports.EMPTY_REPORT));
  }

  @Test
  public void testSlowDiskInfoPBHelper() {
    // Test with a map that has a few slow disk entries.
    final SlowDiskReports slowDisks = SlowDiskReports.create(
        ImmutableMap.of(
            "disk1", ImmutableMap.of(SlowDiskReports.DiskOp.METADATA, 0.5),
            "disk2", ImmutableMap.of(SlowDiskReports.DiskOp.READ, 1.0,
                SlowDiskReports.DiskOp.WRITE, 1.0),
            "disk3", ImmutableMap.of(SlowDiskReports.DiskOp.METADATA, 1.2,
                SlowDiskReports.DiskOp.READ, 1.5,
                SlowDiskReports.DiskOp.WRITE, 1.3)));
    SlowDiskReports slowDisksConverted1 = PBHelper.convertSlowDiskInfo(
        PBHelper.convertSlowDiskInfo(slowDisks));
    assertTrue(
        "Expected map:" + slowDisks + ", got map:" +
            slowDisksConverted1.getSlowDisks(),
        slowDisksConverted1.equals(slowDisks));

    // Test with an empty map
    SlowDiskReports slowDisksConverted2 = PBHelper.convertSlowDiskInfo(
        PBHelper.convertSlowDiskInfo(SlowDiskReports.EMPTY_REPORT));
    assertTrue(
        "Expected empty map:" + ", got map:" + slowDisksConverted2,
        slowDisksConverted2.equals(SlowDiskReports.EMPTY_REPORT));
  }

  private void assertBlockECRecoveryInfoEquals(
      BlockECReconstructionInfo blkECRecoveryInfo1,
      BlockECReconstructionInfo blkECRecoveryInfo2) {
    assertEquals(blkECRecoveryInfo1.getExtendedBlock(),
        blkECRecoveryInfo2.getExtendedBlock());

    DatanodeInfo[] sourceDnInfos1 = blkECRecoveryInfo1.getSourceDnInfos();
    DatanodeInfo[] sourceDnInfos2 = blkECRecoveryInfo2.getSourceDnInfos();
    assertDnInfosEqual(sourceDnInfos1, sourceDnInfos2);

    DatanodeInfo[] targetDnInfos1 = blkECRecoveryInfo1.getTargetDnInfos();
    DatanodeInfo[] targetDnInfos2 = blkECRecoveryInfo2.getTargetDnInfos();
    assertDnInfosEqual(targetDnInfos1, targetDnInfos2);

    String[] targetStorageIDs1 = blkECRecoveryInfo1.getTargetStorageIDs();
    String[] targetStorageIDs2 = blkECRecoveryInfo2.getTargetStorageIDs();
    assertEquals(targetStorageIDs1.length, targetStorageIDs2.length);
    for (int i = 0; i < targetStorageIDs1.length; i++) {
      assertEquals(targetStorageIDs1[i], targetStorageIDs2[i]);
    }

    byte[] liveBlockIndices1 = blkECRecoveryInfo1.getLiveBlockIndices();
    byte[] liveBlockIndices2 = blkECRecoveryInfo2.getLiveBlockIndices();
    for (int i = 0; i < liveBlockIndices1.length; i++) {
      assertEquals(liveBlockIndices1[i], liveBlockIndices2[i]);
    }
    
    ErasureCodingPolicy ecPolicy1 = blkECRecoveryInfo1.getErasureCodingPolicy();
    ErasureCodingPolicy ecPolicy2 = blkECRecoveryInfo2.getErasureCodingPolicy();
    // Compare ECPolicies same as default ECPolicy as we used system default
    // ECPolicy used in this test
    compareECPolicies(StripedFileTestUtil.getDefaultECPolicy(), ecPolicy1);
    compareECPolicies(StripedFileTestUtil.getDefaultECPolicy(), ecPolicy2);
  }

  private void compareECPolicies(ErasureCodingPolicy ecPolicy1, ErasureCodingPolicy ecPolicy2) {
    assertEquals(ecPolicy1.getName(), ecPolicy2.getName());
    assertEquals(ecPolicy1.getNumDataUnits(), ecPolicy2.getNumDataUnits());
    assertEquals(ecPolicy1.getNumParityUnits(), ecPolicy2.getNumParityUnits());
  }

  private void assertDnInfosEqual(DatanodeInfo[] dnInfos1,
      DatanodeInfo[] dnInfos2) {
    assertEquals(dnInfos1.length, dnInfos2.length);
    for (int i = 0; i < dnInfos1.length; i++) {
      compare(dnInfos1[i], dnInfos2[i]);
    }
  }

  /**
   * Test case for old namenode where the namenode doesn't support returning
   * keyProviderUri.
   */
  @Test
  public void testFSServerDefaultsHelper() {
    HdfsProtos.FsServerDefaultsProto.Builder b =
        HdfsProtos.FsServerDefaultsProto
        .newBuilder();
    b.setBlockSize(DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    b.setBytesPerChecksum(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    b.setWritePacketSize(
        HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    b.setReplication(DFSConfigKeys.DFS_REPLICATION_DEFAULT);
    b.setFileBufferSize(DFSConfigKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
    b.setEncryptDataTransfer(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT);
    b.setTrashInterval(DFSConfigKeys.FS_TRASH_INTERVAL_DEFAULT);
    b.setChecksumType(HdfsProtos.ChecksumTypeProto.valueOf(
        DataChecksum.Type.valueOf(DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT).id));
    HdfsProtos.FsServerDefaultsProto proto = b.build();

    assertFalse("KeyProvider uri is not supported",
        proto.hasKeyProviderUri());
    FsServerDefaults fsServerDefaults = PBHelperClient.convert(proto);
    Assert.assertNotNull("FsServerDefaults is null", fsServerDefaults);
    Assert.assertNull("KeyProviderUri should be null",
        fsServerDefaults.getKeyProviderUri());
  }

  @Test
  public void testConvertAddingECPolicyResponse() throws Exception {
    // Check conversion of the built-in policies.
    for (ErasureCodingPolicy policy :
        SystemErasureCodingPolicies.getPolicies()) {
      AddErasureCodingPolicyResponse response =
          new AddErasureCodingPolicyResponse(policy);
      HdfsProtos.AddErasureCodingPolicyResponseProto proto = PBHelperClient
          .convertAddErasureCodingPolicyResponse(response);
      // Optional fields should not be set.
      assertFalse("Unnecessary field is set.", proto.hasErrorMsg());
      // Convert proto back to an object and check for equality.
      AddErasureCodingPolicyResponse convertedResponse = PBHelperClient
          .convertAddErasureCodingPolicyResponse(proto);
      assertEquals("Converted policy not equal", response.getPolicy(),
          convertedResponse.getPolicy());
      assertEquals("Converted policy not equal", response.isSucceed(),
          convertedResponse.isSucceed());
    }

    ErasureCodingPolicy policy = SystemErasureCodingPolicies
        .getPolicies().get(0);
    AddErasureCodingPolicyResponse response =
        new AddErasureCodingPolicyResponse(policy, "failed");
    HdfsProtos.AddErasureCodingPolicyResponseProto proto = PBHelperClient
        .convertAddErasureCodingPolicyResponse(response);
    // Convert proto back to an object and check for equality.
    AddErasureCodingPolicyResponse convertedResponse = PBHelperClient
        .convertAddErasureCodingPolicyResponse(proto);
    assertEquals("Converted policy not equal", response.getPolicy(),
        convertedResponse.getPolicy());
    assertEquals("Converted policy not equal", response.getErrorMsg(),
        convertedResponse.getErrorMsg());
  }

  @Test
  public void testConvertErasureCodingPolicy() throws Exception {
    // Check conversion of the built-in policies.
    for (ErasureCodingPolicy policy :
        SystemErasureCodingPolicies.getPolicies()) {
      HdfsProtos.ErasureCodingPolicyProto proto = PBHelperClient
          .convertErasureCodingPolicy(policy);
      // Optional fields should not be set.
      assertFalse("Unnecessary field is set.", proto.hasName());
      assertFalse("Unnecessary field is set.", proto.hasSchema());
      assertFalse("Unnecessary field is set.", proto.hasCellSize());
      // Convert proto back to an object and check for equality.
      ErasureCodingPolicy convertedPolicy = PBHelperClient
          .convertErasureCodingPolicy(proto);
      assertEquals("Converted policy not equal", policy, convertedPolicy);
    }
    // Check conversion of a non-built-in policy.
    ECSchema newSchema = new ECSchema("testcodec", 3, 2);
    ErasureCodingPolicy newPolicy =
        new ErasureCodingPolicy(newSchema, 128 * 1024);
    HdfsProtos.ErasureCodingPolicyProto proto = PBHelperClient
        .convertErasureCodingPolicy(newPolicy);
    // Optional fields should be set.
    assertTrue("Optional field not set", proto.hasName());
    assertTrue("Optional field not set", proto.hasSchema());
    assertTrue("Optional field not set", proto.hasCellSize());
    ErasureCodingPolicy convertedPolicy = PBHelperClient
        .convertErasureCodingPolicy(proto);
    // Converted policy should be equal.
    assertEquals("Converted policy not equal", newPolicy, convertedPolicy);
  }

  @Test(expected = UninitializedMessageException.class)
  public void testErasureCodingPolicyMissingId() throws Exception {
    HdfsProtos.ErasureCodingPolicyProto.Builder builder =
        HdfsProtos.ErasureCodingPolicyProto.newBuilder();
    PBHelperClient.convertErasureCodingPolicy(builder.build());
  }

  @Test
  public void testErasureCodingPolicyMissingOptionalFields() throws Exception {
    // For non-built-in policies, the optional fields are required
    // when parsing an ErasureCodingPolicyProto.
    HdfsProtos.ECSchemaProto schemaProto =
        PBHelperClient.convertECSchema(
            StripedFileTestUtil.getDefaultECPolicy().getSchema());
    try {
      PBHelperClient.convertErasureCodingPolicy(
          HdfsProtos.ErasureCodingPolicyProto.newBuilder()
              .setId(14)
              .setSchema(schemaProto)
              .setCellSize(123)
              .build());
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Missing", e);
    }
    try {
      PBHelperClient.convertErasureCodingPolicy(
          HdfsProtos.ErasureCodingPolicyProto.newBuilder()
              .setId(14)
              .setName("testpolicy")
              .setCellSize(123)
              .build());
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Missing", e);
    }
    try {
      PBHelperClient.convertErasureCodingPolicy(
          HdfsProtos.ErasureCodingPolicyProto.newBuilder()
              .setId(14)
              .setName("testpolicy")
              .setSchema(schemaProto)
              .build());
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Missing", e);
    }
  }
}
