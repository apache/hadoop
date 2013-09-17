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

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSClusterWithNodeGroup;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Test;
import junit.framework.Assert;

/**
 * This class tests if a balancer schedules tasks correctly.
 */
public class TestBalancerWithNodeGroup {
  private static final Log LOG = LogFactory.getLog(
  "org.apache.hadoop.hdfs.TestBalancerWithNodeGroup");
  
  final private static long CAPACITY = 500L;
  final private static String RACK0 = "/rack0";
  final private static String RACK1 = "/rack1";
  final private static String NODEGROUP0 = "/nodegroup0";
  final private static String NODEGROUP1 = "/nodegroup1";
  final private static String NODEGROUP2 = "/nodegroup2";
  final static private String fileName = "/tmp.txt";
  final static private Path filePath = new Path(fileName);
  MiniDFSClusterWithNodeGroup cluster;

  ClientProtocol client;

  static final long TIMEOUT = 20000L; //msec
  static final double CAPACITY_ALLOWED_VARIANCE = 0.005;  // 0.5%
  static final double BALANCE_ALLOWED_VARIANCE = 0.11;    // 10%+delta
  static final int DEFAULT_BLOCK_SIZE = 10;

  static {
    Balancer.setBlockMoveWaitTime(1000L) ;
  }

  static Configuration createConf() {
    Configuration conf = new HdfsConfiguration();
    TestBalancer.initConf(conf);
    conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_IMPL_KEY, 
        "org.apache.hadoop.net.NetworkTopologyWithNodeGroup");
    conf.set("dfs.block.replicator.classname", 
        "org.apache.hadoop.hdfs.server.blockmanagement." +
        "BlockPlacementPolicyWithNodeGroup");
    return conf;
  }

  /**
   * Wait until heartbeat gives expected results, within CAPACITY_ALLOWED_VARIANCE, 
   * summed over all nodes.  Times out after TIMEOUT msec.
   * @param expectedUsedSpace
   * @param expectedTotalSpace
   * @throws IOException - if getStats() fails
   * @throws TimeoutException
   */
  private void waitForHeartBeat(long expectedUsedSpace, long expectedTotalSpace)
  throws IOException, TimeoutException {
    long timeout = TIMEOUT;
    long failtime = (timeout <= 0L) ? Long.MAX_VALUE
             : System.currentTimeMillis() + timeout;

    while (true) {
      long[] status = client.getStats();
      double totalSpaceVariance = Math.abs((double)status[0] - expectedTotalSpace) 
          / expectedTotalSpace;
      double usedSpaceVariance = Math.abs((double)status[1] - expectedUsedSpace) 
          / expectedUsedSpace;
      if (totalSpaceVariance < CAPACITY_ALLOWED_VARIANCE 
          && usedSpaceVariance < CAPACITY_ALLOWED_VARIANCE)
        break; //done

      if (System.currentTimeMillis() > failtime) {
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
  private void waitForBalancer(long totalUsedSpace, long totalCapacity) 
  throws IOException, TimeoutException {
    long timeout = TIMEOUT;
    long failtime = (timeout <= 0L) ? Long.MAX_VALUE
        : System.currentTimeMillis() + timeout;
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
        if (Math.abs(avgUtilization - nodeUtilization) >
            BALANCE_ALLOWED_VARIANCE) {
          balanced = false;
          if (System.currentTimeMillis() > failtime) {
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

  private void runBalancer(Configuration conf,
      long totalUsedSpace, long totalCapacity) throws Exception {
    waitForHeartBeat(totalUsedSpace, totalCapacity);

    // start rebalancing
    Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
    final int r = Balancer.run(namenodes, Balancer.Parameters.DEFALUT, conf);
    assertEquals(Balancer.ReturnStatus.SUCCESS.code, r);

    waitForHeartBeat(totalUsedSpace, totalCapacity);
    LOG.info("Rebalancing with default factor.");
    waitForBalancer(totalUsedSpace, totalCapacity);
  }
  
  private void runBalancerCanFinish(Configuration conf,
      long totalUsedSpace, long totalCapacity) throws Exception {
    waitForHeartBeat(totalUsedSpace, totalCapacity);

    // start rebalancing
    Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
    final int r = Balancer.run(namenodes, Balancer.Parameters.DEFALUT, conf);
    Assert.assertTrue(r == Balancer.ReturnStatus.SUCCESS.code ||
        (r == Balancer.ReturnStatus.NO_MOVE_PROGRESS.code));
    waitForHeartBeat(totalUsedSpace, totalCapacity);
    LOG.info("Rebalancing with default factor.");
  }

  /**
   * Create a cluster with even distribution, and a new empty node is added to
   * the cluster, then test rack locality for balancer policy. 
   */
  @Test(timeout=60000)
  public void testBalancerWithRackLocality() throws Exception {
    Configuration conf = createConf();
    long[] capacities = new long[]{CAPACITY, CAPACITY};
    String[] racks = new String[]{RACK0, RACK1};
    String[] nodeGroups = new String[]{NODEGROUP0, NODEGROUP1};
    
    int numOfDatanodes = capacities.length;
    assertEquals(numOfDatanodes, racks.length);
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(capacities.length)
                                .racks(racks)
                                .simulatedCapacities(capacities);
    MiniDFSClusterWithNodeGroup.setNodeGroups(nodeGroups);
    cluster = new MiniDFSClusterWithNodeGroup(builder);
    try {
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf, 
          cluster.getFileSystem(0).getUri(),
          ClientProtocol.class).getProxy();

      long totalCapacity = TestBalancer.sum(capacities);

      // fill up the cluster to be 30% full
      long totalUsedSpace = totalCapacity * 3 / 10;
      TestBalancer.createFile(cluster, filePath, totalUsedSpace / numOfDatanodes,
          (short) numOfDatanodes, 0);
      
      long newCapacity = CAPACITY;
      String newRack = RACK1;
      String newNodeGroup = NODEGROUP2;
      // start up an empty node with the same capacity and on the same rack
      cluster.startDataNodes(conf, 1, true, null, new String[]{newRack},
          new long[] {newCapacity}, new String[]{newNodeGroup});

      totalCapacity += newCapacity;

      // run balancer and validate results
      runBalancerCanFinish(conf, totalUsedSpace, totalCapacity);
      
      DatanodeInfo[] datanodeReport = 
              client.getDatanodeReport(DatanodeReportType.ALL);
      
      Map<String, Integer> rackToUsedCapacity = new HashMap<String, Integer>();
      for (DatanodeInfo datanode: datanodeReport) {
        String rack = NetworkTopology.getFirstHalf(datanode.getNetworkLocation());
        int usedCapacity = (int) datanode.getDfsUsed();
         
        if (rackToUsedCapacity.get(rack) != null) {
          rackToUsedCapacity.put(rack, usedCapacity + rackToUsedCapacity.get(rack));
        } else {
          rackToUsedCapacity.put(rack, usedCapacity);
        }
      }
      assertEquals(rackToUsedCapacity.size(), 2);
      assertEquals(rackToUsedCapacity.get(RACK0), rackToUsedCapacity.get(RACK1));
      
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Create a cluster with even distribution, and a new empty node is added to
   * the cluster, then test node-group locality for balancer policy.
   */
  @Test(timeout=60000)
  public void testBalancerWithNodeGroup() throws Exception {
    Configuration conf = createConf();
    long[] capacities = new long[]{CAPACITY, CAPACITY, CAPACITY, CAPACITY};
    String[] racks = new String[]{RACK0, RACK0, RACK1, RACK1};
    String[] nodeGroups = new String[]{NODEGROUP0, NODEGROUP0, NODEGROUP1, NODEGROUP2};
    
    int numOfDatanodes = capacities.length;
    assertEquals(numOfDatanodes, racks.length);
    assertEquals(numOfDatanodes, nodeGroups.length);
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(capacities.length)
                                .racks(racks)
                                .simulatedCapacities(capacities);
    MiniDFSClusterWithNodeGroup.setNodeGroups(nodeGroups);
    cluster = new MiniDFSClusterWithNodeGroup(builder);
    try {
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf, 
          cluster.getFileSystem(0).getUri(),
          ClientProtocol.class).getProxy();

      long totalCapacity = TestBalancer.sum(capacities);
      // fill up the cluster to be 20% full
      long totalUsedSpace = totalCapacity * 2 / 10;
      TestBalancer.createFile(cluster, filePath, totalUsedSpace / (numOfDatanodes/2),
          (short) (numOfDatanodes/2), 0);
      
      long newCapacity = CAPACITY;
      String newRack = RACK1;
      String newNodeGroup = NODEGROUP2;
      // start up an empty node with the same capacity and on NODEGROUP2
      cluster.startDataNodes(conf, 1, true, null, new String[]{newRack},
          new long[] {newCapacity}, new String[]{newNodeGroup});

      totalCapacity += newCapacity;

      // run balancer and validate results
      runBalancer(conf, totalUsedSpace, totalCapacity);

    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Create a 4 nodes cluster: 2 nodes (n0, n1) in RACK0/NODEGROUP0, 1 node (n2)
   * in RACK1/NODEGROUP1 and 1 node (n3) in RACK1/NODEGROUP2. Fill the cluster 
   * to 60% and 3 replicas, so n2 and n3 will have replica for all blocks according
   * to replica placement policy with NodeGroup. As a result, n2 and n3 will be
   * filled with 80% (60% x 4 / 3), and no blocks can be migrated from n2 and n3
   * to n0 or n1 as balancer policy with node group. Thus, we expect the balancer
   * to end in 5 iterations without move block process.
   */
  @Test(timeout=60000)
  public void testBalancerEndInNoMoveProgress() throws Exception {
    Configuration conf = createConf();
    long[] capacities = new long[]{CAPACITY, CAPACITY, CAPACITY, CAPACITY};
    String[] racks = new String[]{RACK0, RACK0, RACK1, RACK1};
    String[] nodeGroups = new String[]{NODEGROUP0, NODEGROUP0, NODEGROUP1, NODEGROUP2};
    
    int numOfDatanodes = capacities.length;
    assertEquals(numOfDatanodes, racks.length);
    assertEquals(numOfDatanodes, nodeGroups.length);
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(capacities.length)
                                .racks(racks)
                                .simulatedCapacities(capacities);
    MiniDFSClusterWithNodeGroup.setNodeGroups(nodeGroups);
    cluster = new MiniDFSClusterWithNodeGroup(builder);
    try {
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf, 
          cluster.getFileSystem(0).getUri(),
          ClientProtocol.class).getProxy();

      long totalCapacity = TestBalancer.sum(capacities);
      // fill up the cluster to be 60% full
      long totalUsedSpace = totalCapacity * 6 / 10;
      TestBalancer.createFile(cluster, filePath, totalUsedSpace / 3, 
          (short) (3), 0);

      // run balancer which can finish in 5 iterations with no block movement.
      runBalancerCanFinish(conf, totalUsedSpace, totalCapacity);

    } finally {
      cluster.shutdown();
    }
  }
}
