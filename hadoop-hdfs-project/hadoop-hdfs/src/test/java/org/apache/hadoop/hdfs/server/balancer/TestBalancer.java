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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
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
import org.apache.hadoop.hdfs.server.balancer.Balancer.Parameters;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Level;
import org.junit.Test;

/**
 * This class tests if a balancer schedules tasks correctly.
 */
public class TestBalancer {
  private static final Log LOG = LogFactory.getLog(
  "org.apache.hadoop.hdfs.TestBalancer");
  static {
    ((Log4JLogger)Balancer.LOG).getLogger().setLevel(Level.ALL);
  }

  final static long CAPACITY = 500L;
  final static String RACK0 = "/rack0";
  final static String RACK1 = "/rack1";
  final static String RACK2 = "/rack2";
  final private static String fileName = "/tmp.txt";
  final static Path filePath = new Path(fileName);
  private MiniDFSCluster cluster;

  ClientProtocol client;

  static final long TIMEOUT = 40000L; //msec
  static final double CAPACITY_ALLOWED_VARIANCE = 0.005;  // 0.5%
  static final double BALANCE_ALLOWED_VARIANCE = 0.11;    // 10%+delta
  static final int DEFAULT_BLOCK_SIZE = 10;
  private static final Random r = new Random();

  static {
    Dispatcher.setBlockMoveWaitTime(1000L) ;
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
      cluster.injectBlocks(i, Arrays.asList(blocksDN[i]), null);

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
      ClientProtocol client, MiniDFSCluster cluster, Balancer.Parameters p)
  throws IOException, TimeoutException {
    waitForBalancer(totalUsedSpace, totalCapacity, client, cluster, p, 0);
  }
  
  /**
   * Wait until balanced: each datanode gives utilization within 
   * BALANCE_ALLOWED_VARIANCE of average
   * @throws IOException
   * @throws TimeoutException
   */
  static void waitForBalancer(long totalUsedSpace, long totalCapacity,
      ClientProtocol client, MiniDFSCluster cluster, Balancer.Parameters p,
      int expectedExcludedNodes) throws IOException, TimeoutException {
    long timeout = TIMEOUT;
    long failtime = (timeout <= 0L) ? Long.MAX_VALUE
        : Time.now() + timeout;
    if (!p.nodesToBeIncluded.isEmpty()) {
      totalCapacity = p.nodesToBeIncluded.size() * CAPACITY;
    }
    if (!p.nodesToBeExcluded.isEmpty()) {
        totalCapacity -= p.nodesToBeExcluded.size() * CAPACITY;
    }
    final double avgUtilization = ((double)totalUsedSpace) / totalCapacity;
    boolean balanced;
    do {
      DatanodeInfo[] datanodeReport = 
          client.getDatanodeReport(DatanodeReportType.ALL);
      assertEquals(datanodeReport.length, cluster.getDataNodes().size());
      balanced = true;
      int actualExcludedNodeCount = 0;
      for (DatanodeInfo datanode : datanodeReport) {
        double nodeUtilization = ((double)datanode.getDfsUsed())
            / datanode.getCapacity();
        if (Dispatcher.Util.isExcluded(p.nodesToBeExcluded, datanode)) {
          assertTrue(nodeUtilization == 0);
          actualExcludedNodeCount++;
          continue;
        }
        if (!Dispatcher.Util.isIncluded(p.nodesToBeIncluded, datanode)) {
          assertTrue(nodeUtilization == 0);
          actualExcludedNodeCount++;
          continue;
        }
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
      assertEquals(expectedExcludedNodes,actualExcludedNodeCount);
    } while (!balanced);
  }

  String long2String(long[] array) {
    if (array.length == 0) {
      return "<empty>";
    }
    StringBuilder b = new StringBuilder("[").append(array[0]);
    for(int i = 1; i < array.length; i++) {
      b.append(", ").append(array[i]);
    }
    return b.append("]").toString();
  }
  /**
   * Class which contains information about the
   * new nodes to be added to the cluster for balancing.
   */
  static abstract class NewNodeInfo {

    Set<String> nodesToBeExcluded = new HashSet<String>();
    Set<String> nodesToBeIncluded = new HashSet<String>();

     abstract String[] getNames();
     abstract int getNumberofNewNodes();
     abstract int getNumberofIncludeNodes();
     abstract int getNumberofExcludeNodes();

     public Set<String> getNodesToBeIncluded() {
       return nodesToBeIncluded;
     }
     public Set<String> getNodesToBeExcluded() {
       return nodesToBeExcluded;
     }
  }

  /**
   * The host names of new nodes are specified
   */
  static class HostNameBasedNodes extends NewNodeInfo {
    String[] hostnames;

    public HostNameBasedNodes(String[] hostnames,
        Set<String> nodesToBeExcluded, Set<String> nodesToBeIncluded) {
      this.hostnames = hostnames;
      this.nodesToBeExcluded = nodesToBeExcluded;
      this.nodesToBeIncluded = nodesToBeIncluded;
    }

    @Override
    String[] getNames() {
      return hostnames;
    }
    @Override
    int getNumberofNewNodes() {
      return hostnames.length;
    }
    @Override
    int getNumberofIncludeNodes() {
      return nodesToBeIncluded.size();
    }
    @Override
    int getNumberofExcludeNodes() {
      return nodesToBeExcluded.size();
    }
  }

  /**
   * The number of data nodes to be started are specified.
   * The data nodes will have same host name, but different port numbers.
   *
   */
  static class PortNumberBasedNodes extends NewNodeInfo {
    int newNodes;
    int excludeNodes;
    int includeNodes;

    public PortNumberBasedNodes(int newNodes, int excludeNodes, int includeNodes) {
      this.newNodes = newNodes;
      this.excludeNodes = excludeNodes;
      this.includeNodes = includeNodes;
    }

    @Override
    String[] getNames() {
      return null;
    }
    @Override
    int getNumberofNewNodes() {
      return newNodes;
    }
    @Override
    int getNumberofIncludeNodes() {
      return includeNodes;
    }
    @Override
    int getNumberofExcludeNodes() {
      return excludeNodes;
    }
  }

  private void doTest(Configuration conf, long[] capacities, String[] racks,
      long newCapacity, String newRack, boolean useTool) throws Exception {
    doTest(conf, capacities, racks, newCapacity, newRack, null, useTool, false);
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
   * @param nodes - information about new nodes to be started.
   * @param useTool - if true run test via Cli with command-line argument 
   *   parsing, etc.   Otherwise invoke balancer API directly.
   * @param useFile - if true, the hosts to included or excluded will be stored in a
   *   file and then later read from the file.
   * @throws Exception
   */
  private void doTest(Configuration conf, long[] capacities,
      String[] racks, long newCapacity, String newRack, NewNodeInfo nodes,
      boolean useTool, boolean useFile) throws Exception {
    LOG.info("capacities = " +  long2String(capacities)); 
    LOG.info("racks      = " +  Arrays.asList(racks)); 
    LOG.info("newCapacity= " +  newCapacity); 
    LOG.info("newRack    = " +  newRack); 
    LOG.info("useTool    = " +  useTool); 
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

      if (nodes == null) { // there is no specification of new nodes.
        // start up an empty node with the same capacity and on the same rack
        cluster.startDataNodes(conf, 1, true, null,
            new String[]{newRack}, null,new long[]{newCapacity});
        totalCapacity += newCapacity;
      } else {
        //if running a test with "include list", include original nodes as well
        if (nodes.getNumberofIncludeNodes()>0) {
          for (DataNode dn: cluster.getDataNodes())
            nodes.getNodesToBeIncluded().add(dn.getDatanodeId().getHostName());
        }
        String[] newRacks = new String[nodes.getNumberofNewNodes()];
        long[] newCapacities = new long[nodes.getNumberofNewNodes()];
        for (int i=0; i < nodes.getNumberofNewNodes(); i++) {
          newRacks[i] = newRack;
          newCapacities[i] = newCapacity;
        }
        // if host names are specified for the new nodes to be created.
        if (nodes.getNames() != null) {
          cluster.startDataNodes(conf, nodes.getNumberofNewNodes(), true, null,
              newRacks, nodes.getNames(), newCapacities);
          totalCapacity += newCapacity*nodes.getNumberofNewNodes();
        } else {  // host names are not specified
          cluster.startDataNodes(conf, nodes.getNumberofNewNodes(), true, null,
              newRacks, null, newCapacities);
          totalCapacity += newCapacity*nodes.getNumberofNewNodes();
          //populate the include nodes
          if (nodes.getNumberofIncludeNodes() > 0) {
            int totalNodes = cluster.getDataNodes().size();
            for (int i=0; i < nodes.getNumberofIncludeNodes(); i++) {
              nodes.getNodesToBeIncluded().add (cluster.getDataNodes().get(
                  totalNodes-1-i).getDatanodeId().getXferAddr());
            }
          }
          //polulate the exclude nodes
          if (nodes.getNumberofExcludeNodes() > 0) {
            int totalNodes = cluster.getDataNodes().size();
            for (int i=0; i < nodes.getNumberofExcludeNodes(); i++) {
              nodes.getNodesToBeExcluded().add (cluster.getDataNodes().get(
                  totalNodes-1-i).getDatanodeId().getXferAddr());
            }
          }
        }
      }
      // run balancer and validate results
      Balancer.Parameters p = Balancer.Parameters.DEFAULT;
      if (nodes != null) {
        p = new Balancer.Parameters(
            Balancer.Parameters.DEFAULT.policy,
            Balancer.Parameters.DEFAULT.threshold,
            nodes.getNodesToBeExcluded(), nodes.getNodesToBeIncluded());
      }

      int expectedExcludedNodes = 0;
      if (nodes != null) {
        if (!nodes.getNodesToBeExcluded().isEmpty()) {
          expectedExcludedNodes = nodes.getNodesToBeExcluded().size();
        } else if (!nodes.getNodesToBeIncluded().isEmpty()) {
          expectedExcludedNodes =
              cluster.getDataNodes().size() - nodes.getNodesToBeIncluded().size();
        }
      }

      // run balancer and validate results
      if (useTool) {
        runBalancerCli(conf, totalUsedSpace, totalCapacity, p, useFile, expectedExcludedNodes);
      } else {
        runBalancer(conf, totalUsedSpace, totalCapacity, p, expectedExcludedNodes);
      }
    } finally {
      cluster.shutdown();
    }
  }

  private void runBalancer(Configuration conf,
      long totalUsedSpace, long totalCapacity) throws Exception {
    runBalancer(conf, totalUsedSpace, totalCapacity, Balancer.Parameters.DEFAULT, 0);
  }

  private void runBalancer(Configuration conf,
     long totalUsedSpace, long totalCapacity, Balancer.Parameters p,
     int excludedNodes) throws Exception {
    waitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);

    // start rebalancing
    Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
    final int r = Balancer.run(namenodes, p, conf);
    if (conf.getInt(DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY, 
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT) ==0) {
      assertEquals(ExitStatus.NO_MOVE_PROGRESS.getExitCode(), r);
      return;
    } else {
      assertEquals(ExitStatus.SUCCESS.getExitCode(), r);
    }
    waitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);
    LOG.info("Rebalancing with default ctor.");
    waitForBalancer(totalUsedSpace, totalCapacity, client, cluster, p, excludedNodes);
  }

  private void runBalancerCli(Configuration conf,
      long totalUsedSpace, long totalCapacity,
      Balancer.Parameters p, boolean useFile, int expectedExcludedNodes) throws Exception {
    waitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);
    List <String> args = new ArrayList<String>();
    args.add("-policy");
    args.add("datanode");

    File excludeHostsFile = null;
    if (!p.nodesToBeExcluded.isEmpty()) {
      args.add("-exclude");
      if (useFile) {
        excludeHostsFile = new File ("exclude-hosts-file");
        PrintWriter pw = new PrintWriter(excludeHostsFile);
        for (String host: p.nodesToBeExcluded) {
          pw.write( host + "\n");
        }
        pw.close();
        args.add("-f");
        args.add("exclude-hosts-file");
      } else {
        args.add(StringUtils.join(p.nodesToBeExcluded, ','));
      }
    }

    File includeHostsFile = null;
    if (!p.nodesToBeIncluded.isEmpty()) {
      args.add("-include");
      if (useFile) {
        includeHostsFile = new File ("include-hosts-file");
        PrintWriter pw = new PrintWriter(includeHostsFile);
        for (String host: p.nodesToBeIncluded){
          pw.write( host + "\n");
        }
        pw.close();
        args.add("-f");
        args.add("include-hosts-file");
      } else {
        args.add(StringUtils.join(p.nodesToBeIncluded, ','));
      }
    }

    final Tool tool = new Cli();    
    tool.setConf(conf);
    final int r = tool.run(args.toArray(new String[0])); // start rebalancing
    
    assertEquals("Tools should exit 0 on success", 0, r);
    waitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);
    LOG.info("Rebalancing with default ctor.");
    waitForBalancer(totalUsedSpace, totalCapacity, client, cluster, p, expectedExcludedNodes);

    if (excludeHostsFile != null && excludeHostsFile.exists()) {
      excludeHostsFile.delete();
    }
    if (includeHostsFile != null && includeHostsFile.exists()) {
      includeHostsFile.delete();
    }
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
  
  /* we first start a cluster and fill the cluster up to a certain size.
   * then redistribute blocks according the required distribution.
   * Then we start an empty datanode.
   * Afterwards a balancer is run to balance the cluster.
   * A partially filled datanode is excluded during balancing.
   * This triggers a situation where one of the block's location is unknown.
   */
  @Test(timeout=100000)
  public void testUnknownDatanode() throws Exception {
    Configuration conf = new HdfsConfiguration();
    initConf(conf);
    long distribution[] = new long[] {50*CAPACITY/100, 70*CAPACITY/100, 0*CAPACITY/100};
    long capacities[] = new long[]{CAPACITY, CAPACITY, CAPACITY};
    String racks[] = new String[] {RACK0, RACK1, RACK1};

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
    try {
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf, cluster.getFileSystem(0).getUri(),
          ClientProtocol.class).getProxy();

      for(int i = 0; i < 3; i++) {
        cluster.injectBlocks(i, Arrays.asList(blocksDN[i]), null);
      }

      cluster.startDataNodes(conf, 1, true, null,
          new String[]{RACK0}, null,new long[]{CAPACITY});
      cluster.triggerHeartbeats();

      Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
      Set<String>  datanodes = new HashSet<String>();
      datanodes.add(cluster.getDataNodes().get(0).getDatanodeId().getHostName());
      Balancer.Parameters p = new Balancer.Parameters(
          Balancer.Parameters.DEFAULT.policy,
          Balancer.Parameters.DEFAULT.threshold,
          datanodes, Balancer.Parameters.DEFAULT.nodesToBeIncluded);
      final int r = Balancer.run(namenodes, p, conf);
      assertEquals(ExitStatus.SUCCESS.getExitCode(), r);
    } finally {
      cluster.shutdown();
    }
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
  public void testBalancerWithZeroThreadsForMove() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY, 0);
    testBalancer1Internal (conf);
  }

  @Test(timeout=100000)
  public void testBalancerWithNonZeroThreadsForMove() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY, 8);
    testBalancer1Internal (conf);
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
   * Verify balancer exits 0 on success.
   */
  @Test(timeout=100000)
  public void testExitZeroOnSuccess() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    
    initConf(conf);
    
    oneNodeTest(conf, true);
  }
  
  /**
   * Test parse method in Balancer#Cli class with wrong number of params
   */

  @Test
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
    parameters = new String[] {"-threshold", "1", "-policy"};
    try {
      Balancer.Cli.parse(parameters);
      fail(reason);
    } catch (IllegalArgumentException e) {

    }
    parameters = new String[] {"-threshold", "1", "-include"};
    try {
      Balancer.Cli.parse(parameters);
      fail(reason);
    } catch (IllegalArgumentException e) {

    }
    parameters = new String[] {"-threshold", "1", "-exclude"};
    try {
      Balancer.Cli.parse(parameters);
      fail(reason);
    } catch (IllegalArgumentException e) {

    }
    parameters = new String[] {"-include",  "-f"};
    try {
      Balancer.Cli.parse(parameters);
      fail(reason);
    } catch (IllegalArgumentException e) {

    }
    parameters = new String[] {"-exclude",  "-f"};
    try {
      Balancer.Cli.parse(parameters);
      fail(reason);
    } catch (IllegalArgumentException e) {

    }

    parameters = new String[] {"-include",  "testnode1", "-exclude", "testnode2"};
    try {
      Balancer.Cli.parse(parameters);
      fail("IllegalArgumentException is expected when both -exclude and -include are specified");
    } catch (IllegalArgumentException e) {

    }
  }


  /**
   * Test a cluster with even distribution,
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the exclude list
   */
  @Test(timeout=100000)
  public void testBalancerWithExcludeList() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    Set<String> excludeHosts = new HashSet<String>();
    excludeHosts.add( "datanodeY");
    excludeHosts.add( "datanodeZ");
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1}, CAPACITY, RACK2,
        new HostNameBasedNodes(new String[] {"datanodeX", "datanodeY", "datanodeZ"},
        excludeHosts, Parameters.DEFAULT.nodesToBeIncluded), false, false);
  }

  /**
   * Test a cluster with even distribution,
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the exclude list
   */
  @Test(timeout=100000)
  public void testBalancerWithExcludeListWithPorts() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1},
        CAPACITY, RACK2, new PortNumberBasedNodes(3, 2, 0), false, false);
  }

  /**
   * Test a cluster with even distribution,
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the exclude list
   */
  @Test(timeout=100000)
  public void testBalancerCliWithExcludeList() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    Set<String> excludeHosts = new HashSet<String>();
    excludeHosts.add( "datanodeY");
    excludeHosts.add( "datanodeZ");
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1}, CAPACITY, RACK2,
      new HostNameBasedNodes(new String[] {"datanodeX", "datanodeY", "datanodeZ"}, excludeHosts,
      Parameters.DEFAULT.nodesToBeIncluded), true, false);
  }

  /**
   * Test a cluster with even distribution,
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the exclude list
   */
  @Test(timeout=100000)
  public void testBalancerCliWithExcludeListWithPorts() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1},
        CAPACITY, RACK2, new PortNumberBasedNodes(3, 2, 0), true, false);
  }

  /**
   * Test a cluster with even distribution,
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the exclude list in a file
   */
  @Test(timeout=100000)
  public void testBalancerCliWithExcludeListInAFile() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    Set<String> excludeHosts = new HashSet<String>();
    excludeHosts.add( "datanodeY");
    excludeHosts.add( "datanodeZ");
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1}, CAPACITY, RACK2,
        new HostNameBasedNodes(new String[] {"datanodeX", "datanodeY", "datanodeZ"},
        excludeHosts, Parameters.DEFAULT.nodesToBeIncluded), true, true);
  }

  /**
   * Test a cluster with even distribution,G
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the exclude list
   */
  @Test(timeout=100000)
  public void testBalancerCliWithExcludeListWithPortsInAFile() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1},
        CAPACITY, RACK2, new PortNumberBasedNodes(3, 2, 0), true, true);
  }

  /**
   * Test a cluster with even distribution,
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the include list
   */
  @Test(timeout=100000)
  public void testBalancerWithIncludeList() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    Set<String> includeHosts = new HashSet<String>();
    includeHosts.add( "datanodeY");
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1}, CAPACITY, RACK2,
        new HostNameBasedNodes(new String[] {"datanodeX", "datanodeY", "datanodeZ"},
        Parameters.DEFAULT.nodesToBeExcluded, includeHosts), false, false);
  }

  /**
   * Test a cluster with even distribution,
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the include list
   */
  @Test(timeout=100000)
  public void testBalancerWithIncludeListWithPorts() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1},
        CAPACITY, RACK2, new PortNumberBasedNodes(3, 0, 1), false, false);
  }

  /**
   * Test a cluster with even distribution,
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the include list
   */
  @Test(timeout=100000)
  public void testBalancerCliWithIncludeList() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    Set<String> includeHosts = new HashSet<String>();
    includeHosts.add( "datanodeY");
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1}, CAPACITY, RACK2,
        new HostNameBasedNodes(new String[] {"datanodeX", "datanodeY", "datanodeZ"},
        Parameters.DEFAULT.nodesToBeExcluded, includeHosts), true, false);
  }

  /**
   * Test a cluster with even distribution,
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the include list
   */
  @Test(timeout=100000)
  public void testBalancerCliWithIncludeListWithPorts() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1},
        CAPACITY, RACK2, new PortNumberBasedNodes(3, 0, 1), true, false);
  }

  /**
   * Test a cluster with even distribution,
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the include list
   */
  @Test(timeout=100000)
  public void testBalancerCliWithIncludeListInAFile() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    Set<String> includeHosts = new HashSet<String>();
    includeHosts.add( "datanodeY");
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1}, CAPACITY, RACK2,
        new HostNameBasedNodes(new String[] {"datanodeX", "datanodeY", "datanodeZ"},
        Parameters.DEFAULT.nodesToBeExcluded, includeHosts), true, true);
  }

  /**
   * Test a cluster with even distribution,
   * then three nodes are added to the cluster,
   * runs balancer with two of the nodes in the include list
   */
  @Test(timeout=100000)
  public void testBalancerCliWithIncludeListWithPortsInAFile() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1},
        CAPACITY, RACK2, new PortNumberBasedNodes(3, 0, 1), true, true);
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
