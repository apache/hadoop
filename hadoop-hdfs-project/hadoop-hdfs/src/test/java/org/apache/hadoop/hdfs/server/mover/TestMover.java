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
package org.apache.hadoop.hdfs.server.mover;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DBlock;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.server.mover.Mover.MLocation;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;

public class TestMover {

  static final int DEFAULT_BLOCK_SIZE = 100;

  static {
    TestBalancer.initTestSetup();
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  static Mover newMover(Configuration conf) throws IOException {
    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    Assert.assertEquals(1, namenodes.size());
    Map<URI, List<Path>> nnMap = Maps.newHashMap();
    for (URI nn : namenodes) {
      nnMap.put(nn, null);
    }

    final List<NameNodeConnector> nncs = NameNodeConnector.newNameNodeConnectors(
        nnMap, Mover.class.getSimpleName(), Mover.MOVER_ID_PATH, conf,
        NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);
    return new Mover(nncs.get(0), conf, new AtomicInteger(0));
  }

  @Test
  public void testScheduleSameBlock() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(4).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testScheduleSameBlock/file";
      
      {
        final FSDataOutputStream out = dfs.create(new Path(file));
        out.writeChars("testScheduleSameBlock");
        out.close();
      }

      final Mover mover = newMover(conf);
      mover.init();
      final Mover.Processor processor = mover.new Processor();

      final LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      final List<MLocation> locations = MLocation.toLocations(lb);
      final MLocation ml = locations.get(0);
      final DBlock db = mover.newDBlock(lb, locations, null);

      final List<StorageType> storageTypes = new ArrayList<StorageType>(
          Arrays.asList(StorageType.DEFAULT, StorageType.DEFAULT));
      Assert.assertTrue(processor.scheduleMoveReplica(db, ml, storageTypes));
      Assert.assertFalse(processor.scheduleMoveReplica(db, ml, storageTypes));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testScheduleBlockWithinSameNode() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[] { StorageType.DISK, StorageType.ARCHIVE })
        .build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testScheduleWithinSameNode/file";
      Path dir = new Path("/testScheduleWithinSameNode");
      dfs.mkdirs(dir);
      // write to DISK
      dfs.setStoragePolicy(dir, "HOT");
      {
        final FSDataOutputStream out = dfs.create(new Path(file));
        out.writeChars("testScheduleWithinSameNode");
        out.close();
      }

      //verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      // move to ARCHIVE
      dfs.setStoragePolicy(dir, "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] { "-p", dir.toString() });
      Assert.assertEquals("Movement to ARCHIVE should be successfull", 0, rc);

      // Wait till namenode notified
      Thread.sleep(3000);
      lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.ARCHIVE == storageType);
      }
    } finally {
      cluster.shutdown();
    }
  }

  private void checkMovePaths(List<Path> actual, Path... expected) {
    Assert.assertEquals(expected.length, actual.size());
    for (Path p : expected) {
      Assert.assertTrue(actual.contains(p));
    }
  }

  /**
   * Test Mover Cli by specifying a list of files/directories using option "-p".
   * There is only one namenode (and hence name service) specified in the conf.
   */
  @Test
  public void testMoverCli() throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(new HdfsConfiguration()).numDataNodes(0).build();
    try {
      final Configuration conf = cluster.getConfiguration(0);
      try {
        Mover.Cli.getNameNodePathsToMove(conf, "-p", "/foo", "bar");
        Assert.fail("Expected exception for illegal path bar");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains("bar is not absolute", e);
      }

      Map<URI, List<Path>> movePaths = Mover.Cli.getNameNodePathsToMove(conf);
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(1, namenodes.size());
      Assert.assertEquals(1, movePaths.size());
      URI nn = namenodes.iterator().next();
      Assert.assertTrue(movePaths.containsKey(nn));
      Assert.assertNull(movePaths.get(nn));

      movePaths = Mover.Cli.getNameNodePathsToMove(conf, "-p", "/foo", "/bar");
      namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(1, movePaths.size());
      nn = namenodes.iterator().next();
      Assert.assertTrue(movePaths.containsKey(nn));
      checkMovePaths(movePaths.get(nn), new Path("/foo"), new Path("/bar"));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverCliWithHAConf() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(new HdfsConfiguration())
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0).build();
    HATestUtil.setFailoverConfigurations(cluster, conf, "MyCluster");
    try {
      Map<URI, List<Path>> movePaths = Mover.Cli.getNameNodePathsToMove(conf,
          "-p", "/foo", "/bar");
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(1, namenodes.size());
      Assert.assertEquals(1, movePaths.size());
      URI nn = namenodes.iterator().next();
      Assert.assertEquals(new URI("hdfs://MyCluster"), nn);
      Assert.assertTrue(movePaths.containsKey(nn));
      checkMovePaths(movePaths.get(nn), new Path("/foo"), new Path("/bar"));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverCliWithFederation() throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(new HdfsConfiguration())
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(3))
        .numDataNodes(0).build();
    final Configuration conf = new HdfsConfiguration();
    DFSTestUtil.setFederatedConfiguration(cluster, conf);
    try {
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(3, namenodes.size());

      try {
        Mover.Cli.getNameNodePathsToMove(conf, "-p", "/foo");
        Assert.fail("Expect exception for missing authority information");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains(
            "does not contain scheme and authority", e);
      }

      try {
        Mover.Cli.getNameNodePathsToMove(conf, "-p", "hdfs:///foo");
        Assert.fail("Expect exception for missing authority information");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains(
            "does not contain scheme and authority", e);
      }

      try {
        Mover.Cli.getNameNodePathsToMove(conf, "-p", "wrong-hdfs://ns1/foo");
        Assert.fail("Expect exception for wrong scheme");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains("Cannot resolve the path", e);
      }

      Iterator<URI> iter = namenodes.iterator();
      URI nn1 = iter.next();
      URI nn2 = iter.next();
      Map<URI, List<Path>> movePaths = Mover.Cli.getNameNodePathsToMove(conf,
          "-p", nn1 + "/foo", nn1 + "/bar", nn2 + "/foo/bar");
      Assert.assertEquals(2, movePaths.size());
      checkMovePaths(movePaths.get(nn1), new Path("/foo"), new Path("/bar"));
      checkMovePaths(movePaths.get(nn2), new Path("/foo/bar"));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverCliWithFederationHA() throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(new HdfsConfiguration())
        .nnTopology(MiniDFSNNTopology.simpleHAFederatedTopology(3))
        .numDataNodes(0).build();
    final Configuration conf = new HdfsConfiguration();
    DFSTestUtil.setFederatedHAConfiguration(cluster, conf);
    try {
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(3, namenodes.size());

      Iterator<URI> iter = namenodes.iterator();
      URI nn1 = iter.next();
      URI nn2 = iter.next();
      URI nn3 = iter.next();
      Map<URI, List<Path>> movePaths = Mover.Cli.getNameNodePathsToMove(conf,
          "-p", nn1 + "/foo", nn1 + "/bar", nn2 + "/foo/bar", nn3 + "/foobar");
      Assert.assertEquals(3, movePaths.size());
      checkMovePaths(movePaths.get(nn1), new Path("/foo"), new Path("/bar"));
      checkMovePaths(movePaths.get(nn2), new Path("/foo/bar"));
      checkMovePaths(movePaths.get(nn3), new Path("/foobar"));
    } finally {
       cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testTwoReplicaSameStorageTypeShouldNotSelect() throws Exception {
    // HDFS-8147
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[][] { { StorageType.DISK, StorageType.ARCHIVE },
                { StorageType.DISK, StorageType.DISK },
                { StorageType.DISK, StorageType.ARCHIVE } }).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testForTwoReplicaSameStorageTypeShouldNotSelect";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file), (short) 2);
      out.writeChars("testForTwoReplicaSameStorageTypeShouldNotSelect");
      out.close();

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] { "-p", file.toString() });
      Assert.assertEquals("Movement to ARCHIVE should be successfull", 0, rc);

      // Wait till namenode notified
      Thread.sleep(3000);
      lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      storageTypes = lb.getStorageTypes();
      int archiveCount = 0;
      for (StorageType storageType : storageTypes) {
        if (StorageType.ARCHIVE == storageType) {
          archiveCount++;
        }
      }
      Assert.assertEquals(archiveCount, 2);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testMoveWhenStoragePolicyNotSatisfying() throws Exception {
    // HDFS-8147
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[][] { { StorageType.DISK }, { StorageType.DISK },
                { StorageType.DISK } }).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoveWhenStoragePolicyNotSatisfying";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file));
      out.writeChars("testMoveWhenStoragePolicyNotSatisfying");
      out.close();

      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] { "-p", file.toString() });
      int exitcode = ExitStatus.NO_MOVE_BLOCK.getExitCode();
      Assert.assertEquals("Exit code should be " + exitcode, exitcode, rc);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverFailedRetry() throws Exception {
    // HDFS-8147
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    conf.set(DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY, "2");
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[][] {{StorageType.DISK, StorageType.ARCHIVE},
                {StorageType.DISK, StorageType.ARCHIVE},
                {StorageType.DISK, StorageType.ARCHIVE}}).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoverFailedRetry";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file), (short) 2);
      out.writeChars("testMoverFailedRetry");
      out.close();

      // Delete block file so, block move will fail with FileNotFoundException
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      cluster.corruptBlockOnDataNodesByDeletingBlockFile(lb.getBlock());
      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", file.toString()});
      Assert.assertEquals("Movement should fail after some retry",
          ExitStatus.NO_MOVE_PROGRESS.getExitCode(), rc);
    } finally {
      cluster.shutdown();
    }
  }

  int dataBlocks = StripedFileTestUtil.NUM_DATA_BLOCKS;
  int parityBlocks = StripedFileTestUtil.NUM_PARITY_BLOCKS;
  private final static int cellSize = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private final static int stripesPerBlock = 4;
  static int DEFAULT_STRIPE_BLOCK_SIZE = cellSize * stripesPerBlock;

  static void initConfWithStripe(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_STRIPE_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, false);
  }

  @Test(timeout = 300000)
  public void testMoverWithStripedFile() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConfWithStripe(conf);

    // start 10 datanodes
    int numOfDatanodes =10;
    int storagesPerDatanode=2;
    long capacity = 10 * DEFAULT_STRIPE_BLOCK_SIZE;
    long[][] capacities = new long[numOfDatanodes][storagesPerDatanode];
    for (int i = 0; i < numOfDatanodes; i++) {
      for(int j=0;j<storagesPerDatanode;j++){
        capacities[i][j]=capacity;
      }
    }
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numOfDatanodes)
        .storagesPerDatanode(storagesPerDatanode)
        .storageTypes(new StorageType[][]{
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE}})
        .storageCapacities(capacities)
        .build();

    try {
      cluster.waitActive();

      // set "/bar" directory with HOT storage policy.
      ClientProtocol client = NameNodeProxies.createProxy(conf,
          cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();
      String barDir = "/bar";
      client.mkdirs(barDir, new FsPermission((short) 777), true);
      client.setStoragePolicy(barDir,
          HdfsConstants.HOT_STORAGE_POLICY_NAME);
      // set an EC policy on "/bar" directory
      client.setErasureCodingPolicy(barDir, null);

      // write file to barDir
      final String fooFile = "/bar/foo";
      long fileLen = 20 * DEFAULT_STRIPE_BLOCK_SIZE ;
      DFSTestUtil.createFile(cluster.getFileSystem(), new Path(fooFile),
          fileLen,(short) 3, 0);

      // verify storage types and locations
      LocatedBlocks locatedBlocks =
          client.getBlockLocations(fooFile, 0, fileLen);
      for(LocatedBlock lb : locatedBlocks.getLocatedBlocks()){
        for( StorageType type : lb.getStorageTypes()){
          Assert.assertEquals(StorageType.DISK, type);
        }
      }
      StripedFileTestUtil.verifyLocatedStripedBlocks(locatedBlocks,
          dataBlocks + parityBlocks);

      // start 5 more datanodes
      numOfDatanodes +=5;
      capacities = new long[5][storagesPerDatanode];
      for (int i = 0; i < 5; i++) {
        for(int j=0;j<storagesPerDatanode;j++){
          capacities[i][j]=capacity;
        }
      }
      cluster.startDataNodes(conf, 5,
          new StorageType[][]{
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE}},
          true, null, null, null,capacities, null, false, false, false, null);
      cluster.triggerHeartbeats();

      // move file to ARCHIVE
      client.setStoragePolicy(barDir, "COLD");
      // run Mover
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] { "-p", barDir });
      Assert.assertEquals("Movement to ARCHIVE should be successfull", 0, rc);

      // verify storage types and locations
      locatedBlocks = client.getBlockLocations(fooFile, 0, fileLen);
      for(LocatedBlock lb : locatedBlocks.getLocatedBlocks()){
        for( StorageType type : lb.getStorageTypes()){
          Assert.assertEquals(StorageType.ARCHIVE, type);
        }
      }
      StripedFileTestUtil.verifyLocatedStripedBlocks(locatedBlocks,
          dataBlocks + parityBlocks);

      // start 5 more datanodes
      numOfDatanodes += 5;
      capacities = new long[5][storagesPerDatanode];
      for (int i = 0; i < 5; i++) {
        for (int j = 0; j < storagesPerDatanode; j++) {
          capacities[i][j] = capacity;
        }
      }
      cluster.startDataNodes(conf, 5,
          new StorageType[][] { { StorageType.SSD, StorageType.DISK },
              { StorageType.SSD, StorageType.DISK },
              { StorageType.SSD, StorageType.DISK },
              { StorageType.SSD, StorageType.DISK },
              { StorageType.SSD, StorageType.DISK } },
          true, null, null, null, capacities, null, false, false, false, null);
      cluster.triggerHeartbeats();

      // move file blocks to ONE_SSD policy
      client.setStoragePolicy(barDir, "ONE_SSD");

      // run Mover
      rc = ToolRunner.run(conf, new Mover.Cli(), new String[] { "-p", barDir });

      // verify storage types and locations
      // Movements should have been ignored for the unsupported policy on
      // striped file
      locatedBlocks = client.getBlockLocations(fooFile, 0, fileLen);
      for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
        for (StorageType type : lb.getStorageTypes()) {
          Assert.assertEquals(StorageType.ARCHIVE, type);
        }
      }
    }finally{
      cluster.shutdown();
    }
  }
}
