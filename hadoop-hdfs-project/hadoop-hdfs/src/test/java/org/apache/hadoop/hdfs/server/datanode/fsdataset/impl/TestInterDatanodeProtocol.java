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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClientAdapter;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaUnderRecovery;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.AutoCloseableLock;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assume.assumeTrue;

/**
 * This tests InterDataNodeProtocol for block handling. 
 */
public class TestInterDatanodeProtocol {
  private static final String ADDRESS = "0.0.0.0";
  final static private int PING_INTERVAL = 1000;
  final static private int MIN_SLEEP_TIME = 1000;
  private static final Configuration conf = new HdfsConfiguration();


  private static class TestServer extends Server {
    private boolean sleep;
    private Class<? extends Writable> responseClass;

    public TestServer(int handlerCount, boolean sleep) throws IOException {
      this(handlerCount, sleep, LongWritable.class, null);
    }

    public TestServer(int handlerCount, boolean sleep,
        Class<? extends Writable> paramClass,
        Class<? extends Writable> responseClass)
      throws IOException {
      super(ADDRESS, 0, paramClass, handlerCount, conf);
      this.sleep = sleep;
      this.responseClass = responseClass;
    }

    @Override
    public Writable call(RPC.RpcKind rpcKind, String protocol, Writable param, long receiveTime)
        throws IOException {
      if (sleep) {
        // sleep a bit
        try {
          Thread.sleep(PING_INTERVAL + MIN_SLEEP_TIME);
        } catch (InterruptedException e) {}
      }
      if (responseClass != null) {
        try {
          return responseClass.newInstance();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        return param;                               // echo param as result
      }
    }
  }

  public static void checkMetaInfo(ExtendedBlock b, DataNode dn) throws IOException {
    Block metainfo = DataNodeTestUtils.getFSDataset(dn).getStoredBlock(
        b.getBlockPoolId(), b.getBlockId());
    Assert.assertEquals(b.getBlockId(), metainfo.getBlockId());
    Assert.assertEquals(b.getNumBytes(), metainfo.getNumBytes());
  }

  public static LocatedBlock getLastLocatedBlock(
      ClientProtocol namenode, String src) throws IOException {
    //get block info for the last block
    LocatedBlocks locations = namenode.getBlockLocations(src, 0, Long.MAX_VALUE);
    List<LocatedBlock> blocks = locations.getLocatedBlocks();
    DataNode.LOG.info("blocks.size()=" + blocks.size());
    assertTrue(blocks.size() > 0);

    return blocks.get(blocks.size() - 1);
  }

  /** Test block MD access via a DN */
  @Test
  public void testBlockMetaDataInfo() throws Exception {
    checkBlockMetaDataInfo(false);
  }

  /** The same as above, but use hostnames for DN<->DN communication */
  @Test
  public void testBlockMetaDataInfoWithHostname() throws Exception {
    assumeTrue(System.getProperty("os.name").startsWith("Linux"));
    checkBlockMetaDataInfo(true);
  }

  /**
   * The following test first creates a file.
   * It verifies the block information from a datanode.
   * Then, it updates the block with new information and verifies again.
   * @param useDnHostname whether DNs should connect to other DNs by hostname
   */
  private void checkBlockMetaDataInfo(boolean useDnHostname) throws Exception {
    MiniDFSCluster cluster = null;

    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME, useDnHostname);
    if (useDnHostname) {
      // Since the mini cluster only listens on the loopback we have to
      // ensure the hostname used to access DNs maps to the loopback. We
      // do this by telling the DN to advertise localhost as its hostname
      // instead of the default hostname.
      conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "localhost");
    }

    try {
      cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .checkDataNodeHostConfig(true)
        .build();
      cluster.waitActive();

      //create a file
      DistributedFileSystem dfs = cluster.getFileSystem();
      String filestr = "/foo";
      Path filepath = new Path(filestr);
      DFSTestUtil.createFile(dfs, filepath, 1024L, (short)3, 0L);
      assertTrue(dfs.exists(filepath));

      //get block info
      LocatedBlock locatedblock = getLastLocatedBlock(
          DFSClientAdapter.getDFSClient(dfs).getNamenode(), filestr);
      DatanodeInfo[] datanodeinfo = locatedblock.getLocations();
      assertTrue(datanodeinfo.length > 0);

      //connect to a data node
      DataNode datanode = cluster.getDataNode(datanodeinfo[0].getIpcPort());
      InterDatanodeProtocol idp = DataNodeTestUtils.createInterDatanodeProtocolProxy(
          datanode, datanodeinfo[0], conf, useDnHostname);
      
      // Stop the block scanners.
      datanode.getBlockScanner().removeAllVolumeScanners();

      //verify BlockMetaDataInfo
      ExtendedBlock b = locatedblock.getBlock();
      InterDatanodeProtocol.LOG.info("b=" + b + ", " + b.getClass());
      checkMetaInfo(b, datanode);
      long recoveryId = b.getGenerationStamp() + 1;
      idp.initReplicaRecovery(
          new RecoveringBlock(b, locatedblock.getLocations(), recoveryId));

      //verify updateBlock
      ExtendedBlock newblock = new ExtendedBlock(b.getBlockPoolId(),
          b.getBlockId(), b.getNumBytes()/2, b.getGenerationStamp()+1);
      idp.updateReplicaUnderRecovery(b, recoveryId, b.getBlockId(),
          newblock.getNumBytes());
      checkMetaInfo(newblock, datanode);
      
      // Verify correct null response trying to init recovery for a missing block
      ExtendedBlock badBlock = new ExtendedBlock("fake-pool",
          b.getBlockId(), 0, 0);
      assertNull(idp.initReplicaRecovery(
          new RecoveringBlock(badBlock,
              locatedblock.getLocations(), recoveryId)));
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  private static ReplicaInfo createReplicaInfo(Block b) {
    return new FinalizedReplica(b, null, null);
  }

  private static void assertEquals(ReplicaInfo originalInfo, ReplicaRecoveryInfo recoveryInfo) {
    Assert.assertEquals(originalInfo.getBlockId(), recoveryInfo.getBlockId());
    Assert.assertEquals(originalInfo.getGenerationStamp(), recoveryInfo.getGenerationStamp());
    Assert.assertEquals(originalInfo.getBytesOnDisk(), recoveryInfo.getNumBytes());
    Assert.assertEquals(originalInfo.getState(), recoveryInfo.getOriginalReplicaState());
  }

  /** Test 
   * {@link FsDatasetImpl#initReplicaRecovery(String, ReplicaMap, Block, long, long)}
   */
  @Test
  public void testInitReplicaRecovery() throws IOException {
    final long firstblockid = 10000L;
    final long gs = 7777L;
    final long length = 22L;
    final ReplicaMap map = new ReplicaMap(new AutoCloseableLock());
    String bpid = "BP-TEST";
    final Block[] blocks = new Block[5];
    for(int i = 0; i < blocks.length; i++) {
      blocks[i] = new Block(firstblockid + i, length, gs);
      map.add(bpid, createReplicaInfo(blocks[i]));
    }
    
    { 
      //normal case
      final Block b = blocks[0];
      final ReplicaInfo originalInfo = map.get(bpid, b);

      final long recoveryid = gs + 1;
      final ReplicaRecoveryInfo recoveryInfo = FsDatasetImpl
          .initReplicaRecovery(bpid, map, blocks[0], recoveryid,
              DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT);
      assertEquals(originalInfo, recoveryInfo);

      final ReplicaUnderRecovery updatedInfo = (ReplicaUnderRecovery)map.get(bpid, b);
      Assert.assertEquals(originalInfo.getBlockId(), updatedInfo.getBlockId());
      Assert.assertEquals(recoveryid, updatedInfo.getRecoveryID());

      //recover one more time 
      final long recoveryid2 = gs + 2;
      final ReplicaRecoveryInfo recoveryInfo2 = FsDatasetImpl
          .initReplicaRecovery(bpid, map, blocks[0], recoveryid2,
              DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT);
      assertEquals(originalInfo, recoveryInfo2);

      final ReplicaUnderRecovery updatedInfo2 = (ReplicaUnderRecovery)map.get(bpid, b);
      Assert.assertEquals(originalInfo.getBlockId(), updatedInfo2.getBlockId());
      Assert.assertEquals(recoveryid2, updatedInfo2.getRecoveryID());
      
      //case RecoveryInProgressException
      try {
        FsDatasetImpl.initReplicaRecovery(bpid, map, b, recoveryid,
            DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT);
        Assert.fail();
      }
      catch(RecoveryInProgressException ripe) {
        System.out.println("GOOD: getting " + ripe);
      }
    }

    { // BlockRecoveryFI_01: replica not found
      final long recoveryid = gs + 1;
      final Block b = new Block(firstblockid - 1, length, gs);
      ReplicaRecoveryInfo r = FsDatasetImpl.initReplicaRecovery(bpid, map, b,
          recoveryid,
          DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT);
      Assert.assertNull("Data-node should not have this replica.", r);
    }
    
    { // BlockRecoveryFI_02: "THIS IS NOT SUPPOSED TO HAPPEN" with recovery id < gs  
      final long recoveryid = gs - 1;
      final Block b = new Block(firstblockid + 1, length, gs);
      try {
        FsDatasetImpl.initReplicaRecovery(bpid, map, b, recoveryid,
            DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT);
        Assert.fail();
      }
      catch(IOException ioe) {
        System.out.println("GOOD: getting " + ioe);
      }
    }

    // BlockRecoveryFI_03: Replica's gs is less than the block's gs
    {
      final long recoveryid = gs + 1;
      final Block b = new Block(firstblockid, length, gs+1);
      try {
        FsDatasetImpl.initReplicaRecovery(bpid, map, b, recoveryid,
            DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT);
        fail("InitReplicaRecovery should fail because replica's " +
        		"gs is less than the block's gs");
      } catch (IOException e) {
        e.getMessage().startsWith(
           "replica.getGenerationStamp() < block.getGenerationStamp(), block=");
      }
    }
  }

  /** 
   * Test  for
   * {@link FsDatasetImpl#updateReplicaUnderRecovery(ExtendedBlock, long, long)} 
   * */
  @Test
  public void testUpdateReplicaUnderRecovery() throws IOException {
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();

      //create a file
      DistributedFileSystem dfs = cluster.getFileSystem();
      String filestr = "/foo";
      Path filepath = new Path(filestr);
      DFSTestUtil.createFile(dfs, filepath, 1024L, (short)3, 0L);

      //get block info
      final LocatedBlock locatedblock = getLastLocatedBlock(
          DFSClientAdapter.getDFSClient(dfs).getNamenode(), filestr);
      final DatanodeInfo[] datanodeinfo = locatedblock.getLocations();
      Assert.assertTrue(datanodeinfo.length > 0);

      //get DataNode and FSDataset objects
      final DataNode datanode = cluster.getDataNode(datanodeinfo[0].getIpcPort());
      Assert.assertTrue(datanode != null);

      //initReplicaRecovery
      final ExtendedBlock b = locatedblock.getBlock();
      final long recoveryid = b.getGenerationStamp() + 1;
      final long newlength = b.getNumBytes() - 1;
      final FsDatasetSpi<?> fsdataset = DataNodeTestUtils.getFSDataset(datanode);
      final ReplicaRecoveryInfo rri = fsdataset.initReplicaRecovery(
          new RecoveringBlock(b, null, recoveryid));

      //check replica
      final Replica replica =
          cluster.getFsDatasetTestUtils(datanode).fetchReplica(b);
      Assert.assertEquals(ReplicaState.RUR, replica.getState());

      //check meta data before update
      cluster.getFsDatasetTestUtils(datanode).checkStoredReplica(replica);

      //case "THIS IS NOT SUPPOSED TO HAPPEN"
      //with (block length) != (stored replica's on disk length). 
      {
        //create a block with same id and gs but different length.
        final ExtendedBlock tmp = new ExtendedBlock(b.getBlockPoolId(), rri
            .getBlockId(), rri.getNumBytes() - 1, rri.getGenerationStamp());
        try {
          //update should fail
          fsdataset.updateReplicaUnderRecovery(tmp, recoveryid,
              tmp.getBlockId(), newlength);
          Assert.fail();
        } catch(IOException ioe) {
          System.out.println("GOOD: getting " + ioe);
        }
      }

      //update
      final Replica r = fsdataset.updateReplicaUnderRecovery(
          new ExtendedBlock(b.getBlockPoolId(), rri), recoveryid,
          rri.getBlockId(), newlength);
      assertTrue(r != null);
      assertTrue(r.getStorageUuid() != null);

    } finally {
      if (cluster != null) cluster.shutdown();
    }
  }

  /** Test to verify that InterDatanode RPC timesout as expected when
   *  the server DN does not respond.
   */
  @Test(expected=SocketTimeoutException.class)
  public void testInterDNProtocolTimeout() throws Throwable {
    final Server server = new TestServer(1, true);
    server.start();

    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    DatanodeID fakeDnId = DFSTestUtil.getLocalDatanodeID(addr.getPort());
    DatanodeInfo dInfo = new DatanodeInfo(fakeDnId);
    InterDatanodeProtocol proxy = null;

    try {
      proxy = DataNode.createInterDataNodeProtocolProxy(
          dInfo, conf, 500, false);
      proxy.initReplicaRecovery(new RecoveringBlock(
          new ExtendedBlock("bpid", 1), null, 100));
      fail ("Expected SocketTimeoutException exception, but did not get.");
    } finally {
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
      server.stop();
    }
  }
}
