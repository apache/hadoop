/*
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

package org.apache.hadoop.hdfs.server.balancer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithUpgradeDomain;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.LazyPersistTestCase;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BALANCER_MAX_SIZE_TO_MOVE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC;
import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Some long running Balancer tasks.
 */
public class TestBalancerLongRunningTasks {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestBalancerLongRunningTasks.class);

  static {
    GenericTestUtils.setLogLevel(Balancer.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(Dispatcher.LOG, Level.DEBUG);
  }

  private final static long CAPACITY = 5000L;
  private final static String RACK0 = "/rack0";
  private final static String RACK1 = "/rack1";
  private final static String RACK2 = "/rack2";
  private final static String FILE_NAME = "/tmp.txt";
  private final static Path FILE_PATH = new Path(FILE_NAME);
  private MiniDFSCluster cluster;

  @After
  public void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private ClientProtocol client;

  static final int DEFAULT_BLOCK_SIZE = 100;
  static final int DEFAULT_RAM_DISK_BLOCK_SIZE = 5 * 1024 * 1024;

  static {
    initTestSetup();
  }

  public static void initTestSetup() {
    // do not create id file since it occupies the disk space
    NameNodeConnector.setWrite2IdFile(false);
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1L);
    SimulatedFSDataset.setFactory(conf);

    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 1L);
    conf.setInt(DFSConfigKeys.DFS_BALANCER_MAX_NO_MOVE_INTERVAL_KEY, 5 * 1000);
  }

  static void initConfWithRamDisk(Configuration conf,
      long ramDiskCapacity) {
    conf.setLong(DFS_BLOCK_SIZE_KEY, DEFAULT_RAM_DISK_BLOCK_SIZE);
    conf.setLong(DFS_DATANODE_MAX_LOCKED_MEMORY_KEY, ramDiskCapacity);
    conf.setInt(DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC, 3);
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setInt(DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC, 1);
    conf.setInt(DFSConfigKeys.DFS_BALANCER_MAX_NO_MOVE_INTERVAL_KEY, 5 * 1000);
    LazyPersistTestCase.initCacheManipulator();

    conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 1L);
  }

  /**
   * Test special case. Two replicas belong to same block should not in same
   * node.
   * We have 2 nodes.
   * We have a block in (DN0,SSD) and (DN1,DISK).
   * Replica in (DN0,SSD) should not be moved to (DN1,SSD).
   * Otherwise DN1 has 2 replicas.
   */
  @Test(timeout = 100000)
  public void testTwoReplicaShouldNotInSameDN() throws Exception {
    final Configuration conf = new HdfsConfiguration();

    int blockSize = 5 * 1024 * 1024;
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1L);

    conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 1L);

    int numOfDatanodes = 2;
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .racks(new String[]{"/default/rack0", "/default/rack0"})
        .storagesPerDatanode(2)
        .storageTypes(new StorageType[][]{
            {StorageType.SSD, StorageType.DISK},
            {StorageType.SSD, StorageType.DISK}})
        .storageCapacities(new long[][]{
            {100 * blockSize, 20 * blockSize},
            {20 * blockSize, 100 * blockSize}})
        .build();
    cluster.waitActive();

    //set "/bar" directory with ONE_SSD storage policy.
    DistributedFileSystem fs = cluster.getFileSystem();
    Path barDir = new Path("/bar");
    fs.mkdir(barDir, new FsPermission((short) 777));
    fs.setStoragePolicy(barDir, HdfsConstants.ONESSD_STORAGE_POLICY_NAME);

    // Insert 30 blocks. So (DN0,SSD) and (DN1,DISK) are about half full,
    // and (DN0,SSD) and (DN1,DISK) are about 15% full.
    long fileLen = 30 * blockSize;
    // fooFile has ONE_SSD policy. So
    // (DN0,SSD) and (DN1,DISK) have 2 replicas belong to same block.
    // (DN0,DISK) and (DN1,SSD) have 2 replicas belong to same block.
    Path fooFile = new Path(barDir, "foo");
    TestBalancer.createFile(cluster, fooFile, fileLen, (short) numOfDatanodes,
        0);
    // update space info
    cluster.triggerHeartbeats();

    BalancerParameters p = BalancerParameters.DEFAULT;
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    final int r = Balancer.run(namenodes, p, conf);

    // Replica in (DN0,SSD) was not moved to (DN1,SSD), because (DN1,DISK)
    // already has one. Otherwise DN1 will have 2 replicas.
    // For same reason, no replicas were moved.
    assertEquals(ExitStatus.NO_MOVE_PROGRESS.getExitCode(), r);
  }

  /*
   * Test Balancer with Ram_Disk configured
   * One DN has two files on RAM_DISK, other DN has no files on RAM_DISK.
   * Then verify that the balancer does not migrate files on RAM_DISK across DN.
   */
  @Test(timeout = 300000)
  public void testBalancerWithRamDisk() throws Exception {
    final int seed = 0xFADED;
    final short replicationFactor = 1;
    Configuration conf = new Configuration();

    final int defaultRamDiskCapacity = 10;
    final long ramDiskStorageLimit =
        ((long) defaultRamDiskCapacity * DEFAULT_RAM_DISK_BLOCK_SIZE) +
            (DEFAULT_RAM_DISK_BLOCK_SIZE - 1);
    final long diskStorageLimit =
        ((long) defaultRamDiskCapacity * DEFAULT_RAM_DISK_BLOCK_SIZE) +
            (DEFAULT_RAM_DISK_BLOCK_SIZE - 1);

    initConfWithRamDisk(conf, ramDiskStorageLimit);

    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(1)
        .storageCapacities(new long[]{ramDiskStorageLimit, diskStorageLimit})
        .storageTypes(new StorageType[]{RAM_DISK, DEFAULT})
        .build();

    cluster.waitActive();
    // Create few files on RAM_DISK
    final String methodName = GenericTestUtils.getMethodName();
    final Path path1 = new Path("/" + methodName + ".01.dat");
    final Path path2 = new Path("/" + methodName + ".02.dat");

    DistributedFileSystem fs = cluster.getFileSystem();
    DFSClient dfsClient = fs.getClient();
    DFSTestUtil.createFile(fs, path1, true,
        DEFAULT_RAM_DISK_BLOCK_SIZE, 4 * DEFAULT_RAM_DISK_BLOCK_SIZE,
        DEFAULT_RAM_DISK_BLOCK_SIZE, replicationFactor, seed, true);
    DFSTestUtil.createFile(fs, path2, true,
        DEFAULT_RAM_DISK_BLOCK_SIZE, 1 * DEFAULT_RAM_DISK_BLOCK_SIZE,
        DEFAULT_RAM_DISK_BLOCK_SIZE, replicationFactor, seed, true);

    // Sleep for a short time to allow the lazy writer thread to do its job
    Thread.sleep(6 * 1000);

    // Add another fresh DN with the same type/capacity without files on
    // RAM_DISK
    StorageType[][] storageTypes = new StorageType[][]{{RAM_DISK, DEFAULT}};
    long[][] storageCapacities = new long[][]{{ramDiskStorageLimit,
            diskStorageLimit}};
    cluster.startDataNodes(conf, replicationFactor, storageTypes, true, null,
        null, null, storageCapacities, null, false, false, false, null, null, null);

    cluster.triggerHeartbeats();
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);

    // Run Balancer
    final BalancerParameters p = BalancerParameters.DEFAULT;
    final int r = Balancer.run(namenodes, p, conf);

    // Validate no RAM_DISK block should be moved
    assertEquals(ExitStatus.NO_MOVE_PROGRESS.getExitCode(), r);

    // Verify files are still on RAM_DISK
    DFSTestUtil.verifyFileReplicasOnStorageType(fs, dfsClient, path1, RAM_DISK);
    DFSTestUtil.verifyFileReplicasOnStorageType(fs, dfsClient, path2, RAM_DISK);
  }

  /**
   * Balancer should not move blocks with size < minBlockSize.
   */
  @Test(timeout = 60000)
  public void testMinBlockSizeAndSourceNodes() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);

    final short replication = 3;
    final long[] lengths = {10, 10, 10, 10};
    final long[] capacities = new long[replication];
    final long totalUsed = capacities.length * TestBalancer.sum(lengths);
    Arrays.fill(capacities, 1000);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(capacities.length)
        .simulatedCapacities(capacities)
        .build();
    final DistributedFileSystem dfs = cluster.getFileSystem();
    cluster.waitActive();
    client = NameNodeProxies.createProxy(conf, dfs.getUri(),
        ClientProtocol.class).getProxy();

    // fill up the cluster to be 80% full
    for (int i = 0; i < lengths.length; i++) {
      final long size = lengths[i];
      final Path p = new Path("/file" + i + "_size" + size);
      try (OutputStream out = dfs.create(p)) {
        for (int j = 0; j < size; j++) {
          out.write(j);
        }
      }
    }

    // start up an empty node with the same capacity
    cluster.startDataNodes(conf, capacities.length, true, null, null, capacities);
    LOG.info("capacities    = " + Arrays.toString(capacities));
    LOG.info("totalUsedSpace= " + totalUsed);
    LOG.info("lengths       = " + Arrays.toString(lengths) + ", #=" + lengths.length);
    TestBalancer.waitForHeartBeat(totalUsed,
        2 * capacities[0] * capacities.length, client, cluster);

    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);

    { // run Balancer with min-block-size=50
      final BalancerParameters p = Balancer.Cli.parse(new String[]{
          "-policy", BalancingPolicy.Node.INSTANCE.getName(),
          "-threshold", "1"
      });
      assertEquals(p.getBalancingPolicy(), BalancingPolicy.Node.INSTANCE);
      assertEquals(p.getThreshold(), 1.0, 0.001);

      conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 50);
      final int r = Balancer.run(namenodes, p, conf);
      assertEquals(ExitStatus.NO_MOVE_PROGRESS.getExitCode(), r);
    }

    conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 1);

    { // run Balancer with empty nodes as source nodes
      final Set<String> sourceNodes = new HashSet<>();
      final List<DataNode> datanodes = cluster.getDataNodes();
      for (int i = capacities.length; i < datanodes.size(); i++) {
        sourceNodes.add(datanodes.get(i).getDisplayName());
      }
      final BalancerParameters p = Balancer.Cli.parse(new String[]{
          "-policy", BalancingPolicy.Node.INSTANCE.getName(),
          "-threshold", "1",
          "-source", StringUtils.join(sourceNodes, ',')
      });
      assertEquals(p.getBalancingPolicy(), BalancingPolicy.Node.INSTANCE);
      assertEquals(p.getThreshold(), 1.0, 0.001);
      assertEquals(p.getSourceNodes(), sourceNodes);

      conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 50);
      final int r = Balancer.run(namenodes, p, conf);
      assertEquals(ExitStatus.NO_MOVE_BLOCK.getExitCode(), r);
    }

    { // run Balancer with a filled node as a source node
      final Set<String> sourceNodes = new HashSet<>();
      final List<DataNode> datanodes = cluster.getDataNodes();
      sourceNodes.add(datanodes.get(0).getDisplayName());
      final BalancerParameters p = Balancer.Cli.parse(new String[]{
          "-policy", BalancingPolicy.Node.INSTANCE.getName(),
          "-threshold", "1",
          "-source", StringUtils.join(sourceNodes, ',')
      });
      assertEquals(p.getBalancingPolicy(), BalancingPolicy.Node.INSTANCE);
      assertEquals(p.getThreshold(), 1.0, 0.001);
      assertEquals(p.getSourceNodes(), sourceNodes);

      conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 1);
      final int r = Balancer.run(namenodes, p, conf);
      assertEquals(ExitStatus.NO_MOVE_BLOCK.getExitCode(), r);
    }

    { // run Balancer with all filled node as source nodes
      final Set<String> sourceNodes = new HashSet<>();
      final List<DataNode> datanodes = cluster.getDataNodes();
      for (int i = 0; i < capacities.length; i++) {
        sourceNodes.add(datanodes.get(i).getDisplayName());
      }
      final BalancerParameters p = Balancer.Cli.parse(new String[]{
          "-policy", BalancingPolicy.Node.INSTANCE.getName(),
          "-threshold", "1",
          "-source", StringUtils.join(sourceNodes, ',')
      });
      assertEquals(p.getBalancingPolicy(), BalancingPolicy.Node.INSTANCE);
      assertEquals(p.getThreshold(), 1.0, 0.001);
      assertEquals(p.getSourceNodes(), sourceNodes);

      conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 1);
      final int r = Balancer.run(namenodes, p, conf);
      assertEquals(ExitStatus.SUCCESS.getExitCode(), r);
    }
  }

  /**
   * Verify balancer won't violate upgrade domain block placement policy.
   *
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void testUpgradeDomainPolicyAfterBalance() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyWithUpgradeDomain.class,
        BlockPlacementPolicy.class);
    long[] capacities = new long[]{CAPACITY, CAPACITY, CAPACITY};
    String[] hosts = {"host0", "host1", "host2"};
    String[] racks = {RACK0, RACK1, RACK1};
    String[] uds = {"ud0", "ud1", "ud2"};
    runBalancerAndVerifyBlockPlacmentPolicy(conf, capacities, hosts, racks,
        uds, CAPACITY, "host3", RACK2, "ud2");
  }

  /**
   * Verify balancer won't violate the default block placement policy.
   *
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void testRackPolicyAfterBalance() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    long[] capacities = new long[]{CAPACITY, CAPACITY};
    String[] hosts = {"host0", "host1"};
    String[] racks = {RACK0, RACK1};
    runBalancerAndVerifyBlockPlacmentPolicy(conf, capacities, hosts, racks,
        null, CAPACITY, "host2", RACK1, null);
  }

  private void runBalancerAndVerifyBlockPlacmentPolicy(Configuration conf,
      long[] capacities, String[] hosts, String[] racks, String[] UDs,
      long newCapacity, String newHost, String newRack, String newUD)
      throws Exception {
    int numOfDatanodes = capacities.length;

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(capacities.length)
        .hosts(hosts).racks(racks).simulatedCapacities(capacities).build();
    DatanodeManager dm = cluster.getNamesystem().getBlockManager().
        getDatanodeManager();
    if (UDs != null) {
      for (int i = 0; i < UDs.length; i++) {
        DatanodeID datanodeId = cluster.getDataNodes().get(i).getDatanodeId();
        dm.getDatanode(datanodeId).setUpgradeDomain(UDs[i]);
      }
    }

    try {
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf,
          cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();

      // fill up the cluster to be 80% full
      long totalCapacity = TestBalancer.sum(capacities);
      long totalUsedSpace = totalCapacity * 8 / 10;

      final long fileSize = totalUsedSpace / numOfDatanodes;
      DFSTestUtil.createFile(cluster.getFileSystem(0), FILE_PATH, false, 1024,
          fileSize, DEFAULT_BLOCK_SIZE, (short) numOfDatanodes, 0, false);

      // start up an empty node with the same capacity on the same rack as the
      // pinned host.
      cluster.startDataNodes(conf, 1, true, null, new String[]{newRack},
          new String[]{newHost}, new long[]{newCapacity});
      if (newUD != null) {
        DatanodeID newId = cluster.getDataNodes().get(
            numOfDatanodes).getDatanodeId();
        dm.getDatanode(newId).setUpgradeDomain(newUD);
      }
      totalCapacity += newCapacity;

      // run balancer and validate results
      TestBalancer.waitForHeartBeat(totalUsedSpace,
          totalCapacity, client, cluster);

      // start rebalancing
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Balancer.run(namenodes, BalancerParameters.DEFAULT, conf);
      BlockPlacementPolicy placementPolicy =
          cluster.getNamesystem().getBlockManager().getBlockPlacementPolicy();
      List<LocatedBlock> locatedBlocks = client.
          getBlockLocations(FILE_NAME, 0, fileSize).getLocatedBlocks();
      for (LocatedBlock locatedBlock : locatedBlocks) {
        BlockPlacementStatus status = placementPolicy.verifyBlockPlacement(
            locatedBlock.getLocations(), numOfDatanodes);
        assertTrue(status.isPlacementPolicySatisfied());
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Make sure that balancer can't move pinned blocks.
   * If specified favoredNodes when create file, blocks will be pinned use
   * sticky bit.
   *
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void testBalancerWithPinnedBlocks() throws Exception {
    // This test assumes stick-bit based block pin mechanism available only
    // in Linux/Unix. It can be unblocked on Windows when HDFS-7759 is ready to
    // provide a different mechanism for Windows.
    assumeNotWindows();

    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    conf.setBoolean(DFS_DATANODE_BLOCK_PINNING_ENABLED, true);

    long[] capacities = new long[]{CAPACITY, CAPACITY};
    String[] hosts = {"host0", "host1"};
    String[] racks = {RACK0, RACK1};
    int numOfDatanodes = capacities.length;

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(capacities.length)
        .hosts(hosts).racks(racks).simulatedCapacities(capacities).build();

    cluster.waitActive();
    client = NameNodeProxies.createProxy(conf,
        cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();

    // fill up the cluster to be 80% full
    long totalCapacity = TestBalancer.sum(capacities);
    long totalUsedSpace = totalCapacity * 8 / 10;
    InetSocketAddress[] favoredNodes = new InetSocketAddress[numOfDatanodes];
    for (int i = 0; i < favoredNodes.length; i++) {
      // DFSClient will attempt reverse lookup. In case it resolves
      // "127.0.0.1" to "localhost", we manually specify the hostname.
      int port = cluster.getDataNodes().get(i).getXferAddress().getPort();
      favoredNodes[i] = new InetSocketAddress(hosts[i], port);
    }

    DFSTestUtil.createFile(cluster.getFileSystem(0), FILE_PATH, false, 1024,
        totalUsedSpace / numOfDatanodes, DEFAULT_BLOCK_SIZE,
        (short) numOfDatanodes, 0, false, favoredNodes);

    // start up an empty node with the same capacity
    cluster.startDataNodes(conf, 1, true, null, new String[]{RACK2},
        new long[]{CAPACITY});

    totalCapacity += CAPACITY;

    // run balancer and validate results
    TestBalancer.waitForHeartBeat(totalUsedSpace, totalCapacity, client,
        cluster);

    // start rebalancing
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    int r = Balancer.run(namenodes, BalancerParameters.DEFAULT, conf);
    assertEquals(ExitStatus.NO_MOVE_PROGRESS.getExitCode(), r);
  }

  @Test(timeout = 60000)
  public void testBalancerWithSortTopNodes() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    conf.setInt(DFS_HEARTBEAT_INTERVAL_KEY, 30000);

    final long capacity = 1000L;
    final int diffBetweenNodes = 50;

    // Set up the datanodes with two groups:
    // 5 over-utilized nodes with 80%, 85%, 90%, 95%, 100% usage
    // 2 under-utilizaed nodes with 0%, 5% usage
    // With sortTopNodes option, 100% and 95% used ones will be chosen.
    final int numOfOverUtilizedDn = 5;
    final int numOfUnderUtilizedDn = 2;
    final int totalNumOfDn = numOfOverUtilizedDn + numOfUnderUtilizedDn;
    final long[] capacityArray = new long[totalNumOfDn];
    Arrays.fill(capacityArray, capacity);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(totalNumOfDn)
        .simulatedCapacities(capacityArray)
        .build();

    cluster.setDataNodesDead();

    List<DataNode> dataNodes = cluster.getDataNodes();

    // Create top used nodes
    for (int i = 0; i < numOfOverUtilizedDn; i++) {
      // Bring one node alive
      DataNodeTestUtils.triggerHeartbeat(dataNodes.get(i));
      DataNodeTestUtils.triggerBlockReport(dataNodes.get(i));
      // Create nodes with: 80%, 85%, 90%, 95%, 100%.
      int capacityForThisDatanode = (int) capacity
          - diffBetweenNodes * (numOfOverUtilizedDn - i - 1);
      TestBalancer.createFile(cluster, new Path("test_big" + i),
          capacityForThisDatanode, (short) 1, 0);
      cluster.setDataNodesDead();
    }

    // Create under utilized nodes
    for (int i = numOfUnderUtilizedDn - 1; i >= 0; i--) {
      int index = i + numOfOverUtilizedDn;
      // Bring one node alive
      DataNodeTestUtils.triggerHeartbeat(dataNodes.get(index));
      DataNodeTestUtils.triggerBlockReport(dataNodes.get(index));
      // Create nodes with: 5%, 0%
      int capacityForThisDatanode = diffBetweenNodes * i;
      TestBalancer.createFile(cluster,
          new Path("test_small" + i),
          capacityForThisDatanode, (short) 1, 0);
      cluster.setDataNodesDead();
    }

    // Bring all nodes alive
    cluster.triggerHeartbeats();
    cluster.triggerBlockReports();
    cluster.waitFirstBRCompleted(0, 6000);

    final BalancerParameters p = Balancer.Cli.parse(new String[]{
        "-policy", BalancingPolicy.Node.INSTANCE.getName(),
        "-threshold", "1",
        "-sortTopNodes"
    });

    client = NameNodeProxies.createProxy(conf,
        cluster.getFileSystem(0).getUri(),
        ClientProtocol.class).getProxy();

    // Set max-size-to-move to small number
    // so only top two nodes will be chosen in one iteration.
    conf.setLong(DFS_BALANCER_MAX_SIZE_TO_MOVE_KEY, 99L);

    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);

    List<NameNodeConnector> connectors = NameNodeConnector
        .newNameNodeConnectors(namenodes,
            Balancer.class.getSimpleName(), Balancer.BALANCER_ID_PATH, conf,
            BalancerParameters.DEFAULT.getMaxIdleIteration());
    final Balancer b = new Balancer(connectors.get(0), p, conf);
    Balancer.Result balancerResult = b.runOneIteration();

    cluster.triggerDeletionReports();
    cluster.triggerBlockReports();
    cluster.triggerHeartbeats();

    DatanodeInfo[] datanodeReport = client
        .getDatanodeReport(HdfsConstants.DatanodeReportType.ALL);

    long maxUsage = 0;
    for (int i = 0; i < totalNumOfDn; i++) {
      maxUsage = Math.max(maxUsage, datanodeReport[i].getDfsUsed());
    }

    // The 95% usage DN will have 9 blocks of 100B and 1 block of 50B - all for the same file.
    // The HDFS balancer will choose a block to move from this node randomly. More likely it will
    // be 100B block. Since 100B is greater than DFS_BALANCER_MAX_SIZE_TO_MOVE_KEY which is 99L,
    // it will stop here. Total bytes moved from this 95% DN will be 1 block of size 100B.
    // However, chances are the first block selected to be moved from this 95% DN is the 50B block.
    // After this block is moved, the total moved size so far would be 50B which is smaller than
    // DFS_BALANCER_MAX_SIZE_TO_MOVE_KEY (99L), hence it will try to move another block.
    // The second block will always be of size 100B. So total bytes moved from this 95% DN will be
    // 2 blocks of size (100B + 50B) 150B.
    // Hence, overall total blocks moved by HDFS balancer would be either of these 2 options:
    // a) 2 blocks of total size (100B + 100B)
    // b) 3 blocks of total size (50B + 100B + 100B)
    assertTrue("BalancerResult is not as expected. " + balancerResult,
        (balancerResult.getBytesAlreadyMoved() == 200
            && balancerResult.getBlocksMoved() == 2)
            || (balancerResult.getBytesAlreadyMoved() == 250
            && balancerResult.getBlocksMoved() == 3));
    // 100% and 95% used nodes will be balanced, so top used will be 900
    assertEquals(900, maxUsage);
  }

  @Test(timeout = 60000)
  public void testBalancerWithLimitOverUtilizedNum() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    // Init the config (block size to 100)
    initConf(conf);
    conf.setInt(DFS_HEARTBEAT_INTERVAL_KEY, 30000);

    final long totalCapacity = 1000L;
    final int diffBetweenNodes = 50;

    // Set up the nodes with two groups:
    // 5 over-utilized nodes with 80%, 85%, 90%, 95%, 100% usage
    // 2 under-utilized nodes with 0%, 5% usage
    // With sortTopNodes and limitOverUtilizedNum option, 100% used ones will be chosen
    final int numOfOverUtilizedDn = 5;
    final int numOfUnderUtilizedDn = 2;
    final int totalNumOfDn = numOfOverUtilizedDn + numOfUnderUtilizedDn;
    final long[] capacityArray = new long[totalNumOfDn];
    Arrays.fill(capacityArray, totalCapacity);

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(totalNumOfDn)
        .simulatedCapacities(capacityArray)
        .build()) {
      cluster.setDataNodesDead();
      List<DataNode> dataNodes = cluster.getDataNodes();
      // Create top used nodes
      for (int i = 0; i < numOfOverUtilizedDn; i++) {
        // Bring one node alive
        DataNodeTestUtils.triggerHeartbeat(dataNodes.get(i));
        DataNodeTestUtils.triggerBlockReport(dataNodes.get(i));
        // Create nodes with: 80%, 85%, 90%, 95%, 100%
        int nodeCapacity = (int) totalCapacity - diffBetweenNodes * (numOfOverUtilizedDn - i - 1);
        TestBalancer.createFile(cluster, new Path("test_big" + i), nodeCapacity, (short) 1, 0);
        cluster.setDataNodesDead();
      }

      // Create under utilized nodes
      for (int i = numOfUnderUtilizedDn - 1; i >= 0; i--) {
        int index = i + numOfOverUtilizedDn;
        // Bring one node alive
        DataNodeTestUtils.triggerHeartbeat(dataNodes.get(index));
        DataNodeTestUtils.triggerBlockReport(dataNodes.get(index));
        // Create nodes with: 5%, 0%
        int nodeCapacity = diffBetweenNodes * i;
        TestBalancer.createFile(cluster, new Path("test_small" + i), nodeCapacity, (short) 1, 0);
        cluster.setDataNodesDead();
      }

      // Bring all nodes alive
      cluster.triggerHeartbeats();
      cluster.triggerBlockReports();
      cluster.waitFirstBRCompleted(0, 6000);

      final BalancerParameters balancerParameters = Balancer.Cli.parse(new String[] {
          "-policy", BalancingPolicy.Node.INSTANCE.getName(),
          "-threshold", "1",
          "-sortTopNodes",
          "-limitOverUtilizedNum", "1"
      });

      client = NameNodeProxies.createProxy(conf, cluster.getFileSystem(0)
              .getUri(), ClientProtocol.class)
              .getProxy();

      // Set max-size-to-move to small number
      // so only top two nodes will be chosen in one iteration
      conf.setLong(DFS_BALANCER_MAX_SIZE_TO_MOVE_KEY, 99L);
      final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      List<NameNodeConnector> connectors =
          NameNodeConnector.newNameNodeConnectors(namenodes, Balancer.class.getSimpleName(),
              Balancer.BALANCER_ID_PATH, conf, BalancerParameters.DEFAULT.getMaxIdleIteration());
      final Balancer balancer = new Balancer(connectors.get(0), balancerParameters, conf);
      Balancer.Result balancerResult = balancer.runOneIteration();

      cluster.triggerDeletionReports();
      cluster.triggerBlockReports();
      cluster.triggerHeartbeats();

      DatanodeInfo[] datanodeReport =
          client.getDatanodeReport(HdfsConstants.DatanodeReportType.ALL);
      long maxUsage = 0;
      for (int i = 0; i < totalNumOfDn; i++) {
        maxUsage = Math.max(maxUsage, datanodeReport[i].getDfsUsed());
      }
      // The maxUsage value is 950, only 100% of the nodes will be balanced
      assertEquals(950, maxUsage);
      assertTrue("BalancerResult is not as expected. " + balancerResult,
          (balancerResult.getBytesAlreadyMoved() == 100 && balancerResult.getBlocksMoved() == 1));
    }
  }

  @Test(timeout = 100000)
  public void testMaxIterationTime() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    int blockSize = 10 * 1024 * 1024; // 10MB block size
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, blockSize);
    // limit the worker thread count of Balancer to have only 1 queue per DN
    conf.setInt(DFSConfigKeys.DFS_BALANCER_MOVERTHREADS_KEY, 1);
    // limit the bandwidth to 4MB per sec to emulate slow block moves
    conf.setLong(DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY,
        4 * 1024 * 1024);
    // set client socket timeout to have an IN_PROGRESS notification back from
    // the DataNode about the copy in every second.
    conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 2000L);
    // set max iteration time to 500 ms to timeout before moving any block
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MAX_ITERATION_TIME_KEY, 500L);
    // setup the cluster
    final long capacity = 10L * blockSize;
    final long[] dnCapacities = new long[]{capacity, capacity};
    final short rep = 1;
    final long seed = 0xFAFAFA;
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0)
        .build();
    try {
      cluster.getConfiguration(0).setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
      conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
      cluster.startDataNodes(conf, 1, true, null, null, dnCapacities);
      cluster.waitClusterUp();
      cluster.waitActive();
      final Path path = new Path("/testMaxIterationTime.dat");
      DistributedFileSystem fs = cluster.getFileSystem();
      // fill the DN to 40%
      DFSTestUtil.createFile(fs, path, 4L * blockSize, rep, seed);
      // start a new DN
      cluster.startDataNodes(conf, 1, true, null, null, dnCapacities);
      cluster.triggerHeartbeats();
      // setup Balancer and run one iteration
      List<NameNodeConnector> connectors = Collections.emptyList();
      try {
        BalancerParameters bParams = BalancerParameters.DEFAULT;
        // set maxIdleIterations to 1 for NO_MOVE_PROGRESS to be
        // reported when there is no block move
        connectors = NameNodeConnector.newNameNodeConnectors(
            DFSUtil.getInternalNsRpcUris(conf), Balancer.class.getSimpleName(),
            Balancer.BALANCER_ID_PATH, conf, 1);
        for (NameNodeConnector nnc : connectors) {
          LOG.info("NNC to work on: " + nnc);
          Balancer b = new Balancer(nnc, bParams, conf);
          Balancer.Result r = b.runOneIteration();
          // Since no block can be moved in 500 milli-seconds (i.e.,
          // 4MB/s * 0.5s = 2MB < 10MB), NO_MOVE_PROGRESS will be reported.
          // When a block move is not canceled in 500 ms properly
          // (highly unlikely) and then a block is moved unexpectedly,
          // IN_PROGRESS will be reported. This is highly unlikely unexpected
          // case. See HDFS-15989.
          assertEquals("We expect ExitStatus.NO_MOVE_PROGRESS to be reported.",
              ExitStatus.NO_MOVE_PROGRESS, r.getExitStatus());
          assertEquals(0, r.getBlocksMoved());
        }
      } finally {
        for (NameNodeConnector nnc : connectors) {
          IOUtils.cleanupWithLogger(null, nnc);
        }
      }
    } finally {
      cluster.shutdown(true, true);
    }
  }

}
