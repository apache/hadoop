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
package org.apache.hadoop.hdfs.server.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.balancer.Balancer.Cli;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.junit.Test;

/**
 * This class tests if a balancer schedules tasks correctly.
 */
public class TestBalancer {
  private static final Log LOG = LogFactory.getLog(
  "org.apache.hadoop.hdfs.TestBalancer");
  
  final static long CAPACITY = 500L;
  final static String RACK0 = "/rack0";
  final static String RACK1 = "/rack1";
  final static String RACK2 = "/rack2";
  final private static String fileName = "/tmp.txt";
  final static Path filePath = new Path(fileName);
  private MiniDFSCluster cluster;

  ClientProtocol client;

  static final long TIMEOUT = 20000L; //msec
  static final double CAPACITY_ALLOWED_VARIANCE = 0.005;  // 0.5%
  static final double BALANCE_ALLOWED_VARIANCE = 0.11;    // 10%+delta
  static final int DEFAULT_BLOCK_SIZE = 10;
  private static final Random r = new Random();

  static {
    Balancer.setBlockMoveWaitTime(1000L) ;
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    SimulatedFSDataset.setFactory(conf);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  /* create a file with a length of <code>fileLen</code> */
  static void createFile(MiniDFSCluster cluster, Path filePath, long fileLen,
      short replicationFactor, int nnIndex)
  throws IOException, InterruptedException, TimeoutException {
    FileSystem fs = cluster.getFileSystem(nnIndex);
    DFSTestUtil.createFile(fs, filePath, fileLen, 
        replicationFactor, r.nextLong());
    DFSTestUtil.waitReplication(fs, filePath, replicationFactor);
  }

  /* fill up a cluster with <code>numNodes</code> datanodes 
   * whose used space to be <code>size</code>
   */
  private ExtendedBlock[] generateBlocks(Configuration conf, long size,
      short numNodes) throws IOException, InterruptedException, TimeoutException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numNodes).build();
    try {
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf, cluster.getFileSystem(0).getUri(),
          ClientProtocol.class).getProxy();

      short replicationFactor = (short)(numNodes-1);
      long fileLen = size/replicationFactor;
      createFile(cluster , filePath, fileLen, replicationFactor, 0);

      List<LocatedBlock> locatedBlocks = client.
      getBlockLocations(fileName, 0, fileLen).getLocatedBlocks();

      int numOfBlocks = locatedBlocks.size();
      ExtendedBlock[] blocks = new ExtendedBlock[numOfBlocks];
      for(int i=0; i<numOfBlocks; i++) {
        ExtendedBlock b = locatedBlocks.get(i).getBlock();
        blocks[i] = new ExtendedBlock(b.getBlockPoolId(), b.getBlockId(), b
            .getNumBytes(), b.getGenerationStamp());
      }

      return blocks;
    } finally {
      cluster.shutdown();
    }
  }

  /* Distribute all blocks according to the given distribution */
  static Block[][] distributeBlocks(ExtendedBlock[] blocks,
      short replicationFactor, final long[] distribution) {
    // make a copy
    long[] usedSpace = new long[distribution.length];
    System.arraycopy(distribution, 0, usedSpace, 0, distribution.length);

    List<List<Block>> blockReports = 
      new ArrayList<List<Block>>(usedSpace.length);
    Block[][] results = new Block[usedSpace.length][];
    for(int i=0; i<usedSpace.length; i++) {
      blockReports.add(new ArrayList<Block>());
    }
    for(int i=0; i<blocks.length; i++) {
      for(int j=0; j<replicationFactor; j++) {
        boolean notChosen = true;
        while(notChosen) {
          int chosenIndex = r.nextInt(usedSpace.length);
          if( usedSpace[chosenIndex]>0 ) {
            notChosen = false;
            blockReports.get(chosenIndex).add(blocks[i].getLocalBlock());
            usedSpace[chosenIndex] -= blocks[i].getNumBytes();
          }
        }
      }
    }
    for(int i=0; i<usedSpace.length; i++) {
      List<Block> nodeBlockList = blockReports.get(i);
      results[i] = nodeBlockList.toArray(new Block[nodeBlockList.size()]);
    }
    return results;
  }

  static long sum(long[] x) {
    long s = 0L;
    for(long a : x) {
      s += a;
    }
    return s;
  }

  /* we first start a cluster and fill the cluster up to a certain size.
   * then redistribute blocks according the required distribution.
   * Afterwards a balancer is running to balance the cluster.
   */
  private void testUnevenDistribution(Configuration conf,
      long distribution[], long capacities[], String[] racks) throws Exception {
    int numDatanodes = distribution.length;
    if (capacities.length != numDatanodes || racks.length != numDatanodes) {
      throw new IllegalArgumentException("Array length is not the same");
    }

    // calculate total space that need to be filled
    final long totalUsedSpace = sum(distribution);

    // fill the cluster
    ExtendedBlock[] blocks = generateBlocks(conf, totalUsedSpace,
        (short) numDatanodes);

    // redistribute blocks
    Block[][] blocksDN = distributeBlocks(
        blocks, (short)(numDatanodes-1), distribution);

    // restart the cluster: do NOT format the cluster
    conf.set(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, "0.0f"); 
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
                                              .format(false)
                                              .racks(racks)
                                              .simulatedCapacities(capacities)
                                              .build();
    cluster.waitActive();
    client = NameNodeProxies.createProxy(conf, cluster.getFileSystem(0).getUri(),
        ClientProtocol.class).getProxy();

    for(int i = 0; i < blocksDN.length; i++)
      cluster.injectBlocks(i, Arrays.asList(blocksDN[i]));

    final long totalCapacity = sum(capacities);
    runBalancer(conf, totalUsedSpace, totalCapacity);
    cluster.shutdown();
  }

  /**
   * Wait until heartbeat gives expected results, within CAPACITY_ALLOWED_VARIANCE, 
   * summed over all nodes.  Times out after TIMEOUT msec.
   * @param expectedUsedSpace
   * @param expectedTotalSpace
   * @throws IOException - if getStats() fails
   * @throws TimeoutException
   */
  static void waitForHeartBeat(long expectedUsedSpace,
      long expectedTotalSpace, ClientProtocol client, MiniDFSCluster cluster)
  throws IOException, TimeoutException {
    long timeout = TIMEOUT;
    long failtime = (timeout <= 0L) ? Long.MAX_VALUE
             : Time.now() + timeout;
    
    while (true) {
      long[] status = client.getStats();
      double totalSpaceVariance = Math.abs((double)status[0] - expectedTotalSpace) 
          / expectedTotalSpace;
      double usedSpaceVariance = Math.abs((double)status[1] - expectedUsedSpace) 
          / expectedUsedSpace;
      if (totalSpaceVariance < CAPACITY_ALLOWED_VARIANCE 
          && usedSpaceVariance < CAPACITY_ALLOWED_VARIANCE)
        break; //done

      if (Time.now() > failtime) {
        throw new TimeoutException("Cluster failed to reached expected values of "
            + "totalSpace (current: " + status[0] 
            + ", expected: " + expectedTotalSpace 
            + "), or usedSpace (current: " + status[1] 
            + ", expected: " + expectedUsedSpace
            + "), in more than " + timeout + " msec.");
      }
      try {
        Thread.sleep(100L);
      } catch(InterruptedException ignored) {
      }
    }
  }
  
  /**
   * Wait until balanced: each datanode gives utilization within 
   * BALANCE_ALLOWED_VARIANCE of average
   * @throws IOException
   * @throws TimeoutException
   */
  static void waitForBalancer(long totalUsedSpace, long totalCapacity,
      ClientProtocol client, MiniDFSCluster cluster)
  throws IOException, TimeoutException {
    long timeout = TIMEOUT;
    long failtime = (timeout <= 0L) ? Long.MAX_VALUE
        : Time.now() + timeout;
    final double avgUtilization = ((double)totalUsedSpace) / totalCapacity;
    boolean balanced;
    do {
      DatanodeInfo[] datanodeReport = 
          client.getDatanodeReport(DatanodeReportType.ALL);
      assertEquals(datanodeReport.length, cluster.getDataNodes().size());
      balanced = true;
      for (DatanodeInfo datanode : datanodeReport) {
        double nodeUtilization = ((double)datanode.getDfsUsed())
            / datanode.getCapacity();
        if (Math.abs(avgUtilization - nodeUtilization) > BALANCE_ALLOWED_VARIANCE) {
          balanced = false;
          if (Time.now() > failtime) {
            throw new TimeoutException(
                "Rebalancing expected avg utilization to become "
                + avgUtilization + ", but on datanode " + datanode
                + " it remains at " + nodeUtilization
                + " after more than " + TIMEOUT + " msec.");
          }
          try {
            Thread.sleep(100);
          } catch (InterruptedException ignored) {
          }
          break;
        }
      }
    } while (!balanced);
  }

  /** This test start a cluster with specified number of nodes, 
   * and fills it to be 30% full (with a single file replicated identically
   * to all datanodes);
   * It then adds one new empty node and starts balancing.
   * 
   * @param conf - configuration
   * @param capacities - array of capacities of original nodes in cluster
   * @param racks - array of racks for original nodes in cluster
   * @param newCapacity - new node's capacity
   * @param newRack - new node's rack
   * @param useTool - if true run test via Cli with command-line argument 
   *   parsing, etc.   Otherwise invoke balancer API directly.
   * @throws Exception
   */
  private void doTest(Configuration conf, long[] capacities, String[] racks, 
      long newCapacity, String newRack, boolean useTool) throws Exception {
    assertEquals(capacities.length, racks.length);
    int numOfDatanodes = capacities.length;
    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(capacities.length)
                                .racks(racks)
                                .simulatedCapacities(capacities)
                                .build();
    try {
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf, cluster.getFileSystem(0).getUri(),
          ClientProtocol.class).getProxy();

      long totalCapacity = sum(capacities);
      
      // fill up the cluster to be 30% full
      long totalUsedSpace = totalCapacity*3/10;
      createFile(cluster, filePath, totalUsedSpace / numOfDatanodes,
          (short) numOfDatanodes, 0);
      // start up an empty node with the same capacity and on the same rack
      cluster.startDataNodes(conf, 1, true, null,
          new String[]{newRack}, new long[]{newCapacity});

      totalCapacity += newCapacity;

      // run balancer and validate results
      if (useTool) {
        runBalancerCli(conf, totalUsedSpace, totalCapacity);
      } else {
        runBalancer(conf, totalUsedSpace, totalCapacity);
      }
    } finally {
      cluster.shutdown();
    }
  }

  private void runBalancer(Configuration conf,
      long totalUsedSpace, long totalCapacity) throws Exception {
    waitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);

    // start rebalancing
    Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
    final int r = Balancer.run(namenodes, Balancer.Parameters.DEFALUT, conf);
    assertEquals(Balancer.ReturnStatus.SUCCESS.code, r);

    waitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);
    LOG.info("Rebalancing with default ctor.");
    waitForBalancer(totalUsedSpace, totalCapacity, client, cluster);
  }
  
  private void runBalancerCli(Configuration conf,
      long totalUsedSpace, long totalCapacity) throws Exception {
    waitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);

    final String[] args = { "-policy", "datanode" };
    final Tool tool = new Cli();    
    tool.setConf(conf);
    final int r = tool.run(args); // start rebalancing
    
    assertEquals("Tools should exit 0 on success", 0, r);
    waitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);
    LOG.info("Rebalancing with default ctor.");
    waitForBalancer(totalUsedSpace, totalCapacity, client, cluster);
  }
  
  /** one-node cluster test*/
  private void oneNodeTest(Configuration conf, boolean useTool) throws Exception {
    // add an empty node with half of the CAPACITY & the same rack
    doTest(conf, new long[]{CAPACITY}, new String[]{RACK0}, CAPACITY/2, 
            RACK0, useTool);
  }
  
  /** two-node cluster test */
  private void twoNodeTest(Configuration conf) throws Exception {
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1},
        CAPACITY, RACK2, false);
  }
  
  /** test using a user-supplied conf */
  public void integrationTest(Configuration conf) throws Exception {
    initConf(conf);
    oneNodeTest(conf, false);
  }
  
  /**
   * Test parse method in Balancer#Cli class with threshold value out of
   * boundaries.
   */
  @Test(timeout=100000)
  public void testBalancerCliParseWithThresholdOutOfBoundaries() {
    String parameters[] = new String[] { "-threshold", "0" };
    String reason = "IllegalArgumentException is expected when threshold value"
        + " is out of boundary.";
    try {
      Balancer.Cli.parse(parameters);
      fail(reason);
    } catch (IllegalArgumentException e) {
      assertEquals("Number out of range: threshold = 0.0", e.getMessage());
    }
    parameters = new String[] { "-threshold", "101" };
    try {
      Balancer.Cli.parse(parameters);
      fail(reason);
    } catch (IllegalArgumentException e) {
      assertEquals("Number out of range: threshold = 101.0", e.getMessage());
    }
  }
  
  /** Test a cluster with even distribution, 
   * then a new empty node is added to the cluster*/
  @Test(timeout=100000)
  public void testBalancer0() throws Exception {
    testBalancer0Internal(new HdfsConfiguration());
  }
  
  void testBalancer0Internal(Configuration conf) throws Exception {
    initConf(conf);
    oneNodeTest(conf, false);
    twoNodeTest(conf);
  }

  /** Test unevenly distributed cluster */
  @Test(timeout=100000)
  public void testBalancer1() throws Exception {
    testBalancer1Internal(new HdfsConfiguration());
  }
  
  void testBalancer1Internal(Configuration conf) throws Exception {
    initConf(conf);
    testUnevenDistribution(conf,
        new long[] {50*CAPACITY/100, 10*CAPACITY/100},
        new long[]{CAPACITY, CAPACITY},
        new String[] {RACK0, RACK1});
  }
  
  @Test(timeout=100000)
  public void testBalancer2() throws Exception {
    testBalancer2Internal(new HdfsConfiguration());
  }
  
  void testBalancer2Internal(Configuration conf) throws Exception {
    initConf(conf);
    testBalancerDefaultConstructor(conf, new long[] { CAPACITY, CAPACITY },
        new String[] { RACK0, RACK1 }, CAPACITY, RACK2);
  }

  private void testBalancerDefaultConstructor(Configuration conf,
      long[] capacities, String[] racks, long newCapacity, String newRack)
      throws Exception {
    int numOfDatanodes = capacities.length;
    assertEquals(numOfDatanodes, racks.length);
    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(capacities.length)
                                .racks(racks)
                                .simulatedCapacities(capacities)
                                .build();
    try {
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf, cluster.getFileSystem(0).getUri(),
          ClientProtocol.class).getProxy();

      long totalCapacity = sum(capacities);

      // fill up the cluster to be 30% full
      long totalUsedSpace = totalCapacity * 3 / 10;
      createFile(cluster, filePath, totalUsedSpace / numOfDatanodes,
          (short) numOfDatanodes, 0);
      // start up an empty node with the same capacity and on the same rack
      cluster.startDataNodes(conf, 1, true, null, new String[] { newRack },
          new long[] { newCapacity });

      totalCapacity += newCapacity;

      // run balancer and validate results
      runBalancer(conf, totalUsedSpace, totalCapacity);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test parse method in Balancer#Cli class with wrong number of params
   */
  @Test(timeout=100000)
  public void testBalancerCliParseWithWrongParams() {
    String parameters[] = new String[] { "-threshold" };
    String reason =
        "IllegalArgumentException is expected when value is not specified";
    try {
      Balancer.Cli.parse(parameters);
      fail(reason);
    } catch (IllegalArgumentException e) {

    }
    parameters = new String[] { "-policy" };
    try {
      Balancer.Cli.parse(parameters);
      fail(reason);
    } catch (IllegalArgumentException e) {

    }
    parameters = new String[] { "-threshold 1 -policy" };
    try {
      Balancer.Cli.parse(parameters);
      fail(reason);
    } catch (IllegalArgumentException e) {

    }

  }

  /**
   * Verify balancer exits 0 on success.
   */
  @Test(timeout=100000)
  public void testExitZeroOnSuccess() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    
    initConf(conf);
    
    oneNodeTest(conf, true);
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    TestBalancer balancerTest = new TestBalancer();
    balancerTest.testBalancer0();
    balancerTest.testBalancer1();
    balancerTest.testBalancer2();
  }
}
