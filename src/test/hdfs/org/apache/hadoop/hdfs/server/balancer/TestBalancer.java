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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;
/**
 * This class tests if a balancer schedules tasks correctly.
 */
public class TestBalancer extends TestCase {
  private static final Configuration CONF = new Configuration();
  final private static long CAPACITY = 500L;
  final private static String RACK0 = "/rack0";
  final private static String RACK1 = "/rack1";
  final private static String RACK2 = "/rack2";
  final static private String fileName = "/tmp.txt";
  final static private Path filePath = new Path(fileName);
  private MiniDFSCluster cluster;

  ClientProtocol client;

  static final int DEFAULT_BLOCK_SIZE = 10;
  private Balancer balancer;
  private Random r = new Random();

  static {
    CONF.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    CONF.setInt("io.bytes.per.checksum", DEFAULT_BLOCK_SIZE);
    CONF.setLong("dfs.heartbeat.interval", 1L);
    CONF.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    CONF.setLong("dfs.balancer.movedWinWidth", 2000L);
    Balancer.setBlockMoveWaitTime(1000L) ;
  }

  /* create a file with a length of <code>fileLen</code> */
  private void createFile(long fileLen, short replicationFactor)
  throws IOException {
    FileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, filePath, fileLen, 
        replicationFactor, r.nextLong());
    DFSTestUtil.waitReplication(fs, filePath, replicationFactor);
  }


  /* fill up a cluster with <code>numNodes</code> datanodes 
   * whose used space to be <code>size</code>
   */
  private Block[] generateBlocks(long size, short numNodes) throws IOException {
    cluster = new MiniDFSCluster( CONF, numNodes, true, null);
    try {
      cluster.waitActive();
      client = DFSClient.createNamenode(CONF);

      short replicationFactor = (short)(numNodes-1);
      long fileLen = size/replicationFactor;
      createFile(fileLen, replicationFactor);

      List<LocatedBlock> locatedBlocks = client.
      getBlockLocations(fileName, 0, fileLen).getLocatedBlocks();

      int numOfBlocks = locatedBlocks.size();
      Block[] blocks = new Block[numOfBlocks];
      for(int i=0; i<numOfBlocks; i++) {
        Block b = locatedBlocks.get(i).getBlock();
        blocks[i] = new Block(b.getBlockId(), b.getNumBytes(), b.getGenerationStamp());
      }

      return blocks;
    } finally {
      cluster.shutdown();
    }
  }

  /* Distribute all blocks according to the given distribution */
  Block[][] distributeBlocks(Block[] blocks, short replicationFactor, 
      final long[] distribution ) {
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
            blockReports.get(chosenIndex).add(blocks[i]);
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

  /* we first start a cluster and fill the cluster up to a certain size.
   * then redistribute blocks according the required distribution.
   * Afterwards a balancer is running to balance the cluster.
   */
  private void testUnevenDistribution(
      long distribution[], long capacities[], String[] racks) throws Exception {
    int numDatanodes = distribution.length;
    if (capacities.length != numDatanodes || racks.length != numDatanodes) {
      throw new IllegalArgumentException("Array length is not the same");
    }

    // calculate total space that need to be filled
    long totalUsedSpace=0L;
    for(int i=0; i<distribution.length; i++) {
      totalUsedSpace += distribution[i];
    }

    // fill the cluster
    Block[] blocks = generateBlocks(totalUsedSpace, (short)numDatanodes);

    // redistribute blocks
    Block[][] blocksDN = distributeBlocks(
        blocks, (short)(numDatanodes-1), distribution);

    // restart the cluster: do NOT format the cluster
    CONF.set("dfs.safemode.threshold.pct", "0.0f"); 
    cluster = new MiniDFSCluster(0, CONF, numDatanodes,
        false, true, null, racks, capacities);
    cluster.waitActive();
    client = DFSClient.createNamenode(CONF);

    cluster.injectBlocks(blocksDN);

    long totalCapacity = 0L;
    for(long capacity:capacities) {
      totalCapacity += capacity;
    }
    runBalancer(totalUsedSpace, totalCapacity);
  }

  /* wait for one heartbeat */
  private void waitForHeartBeat( long expectedUsedSpace, long expectedTotalSpace )
  throws IOException {
    long[] status = client.getStats();
    while(status[0] != expectedTotalSpace || status[1] != expectedUsedSpace ) {
      try {
        Thread.sleep(100L);
      } catch(InterruptedException ignored) {
      }
      status = client.getStats();
    }
  }

  /* This test start a one-node cluster, fill the node to be 30% full;
   * It then adds an empty node and start balancing.
   * @param newCapacity new node's capacity
   * @param new 
   */
  private void test(long[] capacities, String[] racks, 
      long newCapacity, String newRack) throws Exception {
    int numOfDatanodes = capacities.length;
    assertEquals(numOfDatanodes, racks.length);
    cluster = new MiniDFSCluster(0, CONF, capacities.length, true, true, null, 
        racks, capacities);
    try {
      cluster.waitActive();
      client = DFSClient.createNamenode(CONF);

      long totalCapacity=0L;
      for(long capacity:capacities) {
        totalCapacity += capacity;
      }
      // fill up the cluster to be 30% full
      long totalUsedSpace = totalCapacity*3/10;
      createFile(totalUsedSpace/numOfDatanodes, (short)numOfDatanodes);
      // start up an empty node with the same capacity and on the same rack
      cluster.startDataNodes(CONF, 1, true, null,
          new String[]{newRack}, new long[]{newCapacity});

      totalCapacity += newCapacity;

      // run balancer and validate results
      runBalancer(totalUsedSpace, totalCapacity);
    } finally {
      cluster.shutdown();
    }
  }

  /* Start balancer and check if the cluster is balanced after the run */
  private void runBalancer( long totalUsedSpace, long totalCapacity )
  throws Exception {
    waitForHeartBeat(totalUsedSpace, totalCapacity);

    // start rebalancing
    balancer = new Balancer(CONF);
    balancer.run(new String[0]);

    waitForHeartBeat(totalUsedSpace, totalCapacity);
    boolean balanced;
    do {
      DatanodeInfo[] datanodeReport = 
        client.getDatanodeReport(DatanodeReportType.ALL);
      assertEquals(datanodeReport.length, cluster.getDataNodes().size());
      balanced = true;
      double avgUtilization = ((double)totalUsedSpace)/totalCapacity*100;
      for(DatanodeInfo datanode:datanodeReport) {
        if(Math.abs(avgUtilization-
            ((double)datanode.getDfsUsed())/datanode.getCapacity()*100)>10) {
          balanced = false;
          try {
            Thread.sleep(100);
          } catch(InterruptedException ignored) {
          }
          break;
        }
      }
    } while(!balanced);

  }
  /** Test a cluster with even distribution, 
   * then a new empty node is added to the cluster*/
  public void testBalancer0() throws Exception {
    /** one-node cluster test*/
    // add an empty node with half of the CAPACITY & the same rack
    test(new long[]{CAPACITY}, new String[]{RACK0}, CAPACITY/2, RACK0);

    /** two-node cluster test */
    test(new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1},
        CAPACITY, RACK2);
  }

  /** Test unevenly distributed cluster */
  public void testBalancer1() throws Exception {
    testUnevenDistribution(
        new long[] {50*CAPACITY/100, 10*CAPACITY/100},
        new long[]{CAPACITY, CAPACITY},
        new String[] {RACK0, RACK1});
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    TestBalancer balancerTest = new TestBalancer();
    balancerTest.testBalancer0();
    balancerTest.testBalancer1();
  }
}
