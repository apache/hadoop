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
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.balancer.BalancerParameters;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.event.Level;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test balancer with multiple NameNodes
 */
public class TestBalancerWithMultipleNameNodes {
  static final Logger LOG = Balancer.LOG;
  {
    GenericTestUtils.setLogLevel(LOG, Level.TRACE);
    DFSTestUtil.setNameNodeLogLevel(org.apache.log4j.Level.TRACE);
  }

  
  private static final long CAPACITY = 500L;
  private static final String RACK0 = "/rack0";
  private static final String RACK1 = "/rack1";
  private static final String RACK2 = "/rack2";

  private static final String FILE_NAME = "/tmp.txt";
  private static final Path FILE_PATH = new Path(FILE_NAME);
  
  private static final Random RANDOM = new Random();

  static {
    TestBalancer.initTestSetup();
  }

  /** Common objects used in various methods. */
  private static class Suite {
    final Configuration conf;
    final MiniDFSCluster cluster;
    final ClientProtocol[] clients;
    final short replication;
    final BalancerParameters parameters;

    Suite(MiniDFSCluster cluster, final int nNameNodes, final int nDataNodes,
        BalancerParameters parameters, Configuration conf) throws IOException {
      this.conf = conf;
      this.cluster = cluster;
      clients = new ClientProtocol[nNameNodes];
      for(int i = 0; i < nNameNodes; i++) {
        clients[i] = cluster.getNameNode(i).getRpcServer();
      }
      // hard coding replication factor to 1 so logical and raw HDFS size are
      // equal
      replication = 1;
      this.parameters = parameters;
    }

    Suite(MiniDFSCluster cluster, final int nNameNodes, final int nDataNodes,
          BalancerParameters parameters, Configuration conf, short
              replicationFactor) throws IOException {
      this.conf = conf;
      this.cluster = cluster;
      clients = new ClientProtocol[nNameNodes];
      for(int i = 0; i < nNameNodes; i++) {
        clients[i] = cluster.getNameNode(i).getRpcServer();
      }
      replication = replicationFactor;
      this.parameters = parameters;
    }
  }

  /* create a file with a length of <code>fileLen</code> */
  private static void createFile(Suite s, int index, long len
      ) throws IOException, InterruptedException, TimeoutException {
    final FileSystem fs = s.cluster.getFileSystem(index);
    DFSTestUtil.createFile(fs, FILE_PATH, len, s.replication, RANDOM.nextLong());
    DFSTestUtil.waitReplication(fs, FILE_PATH, s.replication);
  }

  /* fill up a cluster with <code>numNodes</code> datanodes 
   * whose used space to be <code>size</code>
   */
  private static ExtendedBlock[][] generateBlocks(Suite s, long size
      ) throws IOException, InterruptedException, TimeoutException {
    final ExtendedBlock[][] blocks = new ExtendedBlock[s.clients.length][];
    for(int n = 0; n < s.clients.length; n++) {
      createFile(s, n, size);
      final List<LocatedBlock> locatedBlocks =
          s.clients[n].getBlockLocations(FILE_NAME, 0, size).getLocatedBlocks();

      final int numOfBlocks = locatedBlocks.size();
      blocks[n] = new ExtendedBlock[numOfBlocks];
      for(int i = 0; i < numOfBlocks; i++) {
        final ExtendedBlock b = locatedBlocks.get(i).getBlock();
        blocks[n][i] = new ExtendedBlock(b.getBlockPoolId(), b.getBlockId(),
            b.getNumBytes(), b.getGenerationStamp());
      }
    }
    return blocks;
  }

  /* wait for one heartbeat */
  static void wait(final Suite suite,
      long expectedUsedSpace, long expectedTotalSpace) throws IOException {
    LOG.info("WAIT expectedUsedSpace=" + expectedUsedSpace
        + ", expectedTotalSpace=" + expectedTotalSpace);
    suite.cluster.triggerHeartbeats();
    for(int n = 0; n < suite.clients.length; n++) {
      int i = 0;
      for(boolean done = false; !done; ) {
        final long[] s = suite.clients[n].getStats();
        done = s[0] == expectedTotalSpace && s[1] >= expectedUsedSpace;
        if (!done) {
          sleep(100L);
          if (++i % 100 == 0) {
            LOG.warn("WAIT i=" + i + ", s=[" + s[0] + ", " + s[1] + "]");
          }
        }
      }
    }
  }

  static void runBalancer(Suite s,
      final long totalUsed, final long totalCapacity) throws Exception {
    double avg = totalUsed*100.0/totalCapacity;

    LOG.info("BALANCER 0: totalUsed=" + totalUsed
        + ", totalCapacity=" + totalCapacity
        + ", avg=" + avg);
    wait(s, totalUsed, totalCapacity);
    LOG.info("BALANCER 1");

    // get storage reports for relevant blockpools so that we can compare
    // blockpool usages after balancer has run
    Map<Integer, DatanodeStorageReport[]> preBalancerPoolUsages =
        getStorageReports(s);

    // start rebalancing
    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(s.conf);
    final int r = Balancer.run(namenodes, s.parameters, s.conf);
    Assert.assertEquals(ExitStatus.SUCCESS.getExitCode(), r);

    LOG.info("BALANCER 2");
    wait(s, totalUsed, totalCapacity);
    LOG.info("BALANCER 3");

    int i = 0;
    for(boolean balanced = false; !balanced; i++) {
      final long[] used = new long[s.cluster.getDataNodes().size()];
      final long[] cap = new long[used.length];
      final long[][] bpUsed = new long[s.clients.length][s.cluster
          .getDataNodes().size()];


      for(int n = 0; n < s.clients.length; n++) {
        final DatanodeInfo[] datanodes = s.clients[n].getDatanodeReport(
            DatanodeReportType.ALL);
        Assert.assertEquals(datanodes.length, used.length);

        for(int d = 0; d < datanodes.length; d++) {
          if (n == 0) {
            used[d] = datanodes[d].getDfsUsed();
            cap[d] = datanodes[d].getCapacity();
            if (i % 100 == 0) {
              LOG.warn("datanodes[" + d
                  + "]: getDfsUsed()=" + datanodes[d].getDfsUsed()
                  + ", getCapacity()=" + datanodes[d].getCapacity());
            }
          } else {
            Assert.assertEquals(used[d], datanodes[d].getDfsUsed());
            Assert.assertEquals(cap[d], datanodes[d].getCapacity());
          }
          bpUsed[n][d] = datanodes[d].getBlockPoolUsed();
        }
      }



      balanced = true;
      for(int d = 0; d < used.length; d++) {
        double p;
        if(s.parameters.getBalancingPolicy() == BalancingPolicy.Pool.INSTANCE) {
          for (int k = 0; k < s.parameters.getBlockPools().size(); k++) {
            avg = TestBalancer.sum(bpUsed[k])*100/totalCapacity;
            p = bpUsed[k][d] * 100.0 / cap[d];
            balanced = p <= avg + s.parameters.getThreshold();
            if (!balanced) {
              if (i % 100 == 0) {
                LOG.warn("datanodes " + d + " is not yet balanced: "
                    + "block pool used=" + bpUsed[d][k] + ", cap=" + cap[d] +
                    ", avg=" + avg);
                LOG.warn("sum(blockpoolUsed)=" + TestBalancer.sum(bpUsed[k])
                    + ", sum(cap)=" + TestBalancer.sum(cap));
              }
              sleep(100);
              break;
            }
          }
          if (!balanced) {
            break;
          }
        } else {
          p = used[d] * 100.0 / cap[d];
          balanced = p <= avg + s.parameters.getThreshold();
          if (!balanced) {
            if (i % 100 == 0) {
              LOG.warn("datanodes " + d + " is not yet balanced: "
                  + "used=" + used[d] + ", cap=" + cap[d] + ", avg=" + avg);
              LOG.warn("sum(used)=" + TestBalancer.sum(used)
                  + ", sum(cap)=" + TestBalancer.sum(cap));
            }
            sleep(100);
            break;
          }
        }
      }
    }

    LOG.info("BALANCER 6");
    // cluster is balanced, verify that only selected blockpools were touched
    Map<Integer, DatanodeStorageReport[]> postBalancerPoolUsages =
        getStorageReports(s);
    Assert.assertEquals(preBalancerPoolUsages.size(),
        postBalancerPoolUsages.size());
    for (Map.Entry<Integer, DatanodeStorageReport[]> entry
        : preBalancerPoolUsages.entrySet()) {
      compareTotalPoolUsage(entry.getValue(),
          postBalancerPoolUsages.get(entry.getKey()));
    }
  }

  /**
   * Compare the total blockpool usage on each datanode to ensure that nothing
   * was balanced.
   *
   * @param preReports storage reports from pre balancer run
   * @param postReports storage reports from post balancer run
   */
  private static void compareTotalPoolUsage(DatanodeStorageReport[] preReports,
      DatanodeStorageReport[] postReports) {
    Assert.assertNotNull(preReports);
    Assert.assertNotNull(postReports);
    Assert.assertEquals(preReports.length, postReports.length);
    for (DatanodeStorageReport preReport : preReports) {
      String dnUuid = preReport.getDatanodeInfo().getDatanodeUuid();
      for(DatanodeStorageReport postReport : postReports) {
        if(postReport.getDatanodeInfo().getDatanodeUuid().equals(dnUuid)) {
          Assert.assertEquals(getTotalPoolUsage(preReport),
              getTotalPoolUsage(postReport));
          LOG.info("Comparision of datanode pool usage pre/post balancer run. "
              + "PrePoolUsage: " + getTotalPoolUsage(preReport)
              + ", PostPoolUsage: " + getTotalPoolUsage(postReport));
          break;
        }
      }
    }
  }

  private static long getTotalPoolUsage(DatanodeStorageReport report) {
    long usage = 0L;
    for (StorageReport sr : report.getStorageReports()) {
      usage += sr.getBlockPoolUsed();
    }
    return usage;
  }

  /**
   * Get the storage reports for all blockpools that were not specified by the
   * balancer blockpool parameters. If none were specified then the parameter
   * was not set and do not return any reports.
   *
   * @param s suite for the test
   * @return a map of storage reports where the key is the blockpool index
   * @throws IOException
   */
  private static Map<Integer,
    DatanodeStorageReport[]> getStorageReports(Suite s) throws IOException {
    Map<Integer, DatanodeStorageReport[]> reports =
        new HashMap<Integer, DatanodeStorageReport[]>();
    if (s.parameters.getBlockPools().size() == 0) {
      // the blockpools parameter was not set, so we don't need to track any
      // blockpools.
      return Collections.emptyMap();
    }
    for (int i = 0; i < s.clients.length; i++) {
      if (s.parameters.getBlockPools().contains(
          s.cluster.getNamesystem(i)
          .getBlockPoolId())) {
        // we want to ensure that blockpools not specified by the balancer
        // parameters were left alone. Therefore, if the pool was specified,
        // skip it. Note: this code assumes the clients in the suite are ordered
        // the same way that they are indexed via cluster#getNamesystem(index).
        continue;
      } else {
        LOG.info("Tracking usage of blockpool id: "
            + s.cluster.getNamesystem(i).getBlockPoolId());
        reports.put(i,
            s.clients[i].getDatanodeStorageReport(DatanodeReportType.LIVE));
      }
    }
    LOG.info("Tracking " + reports.size()
        + " blockpool(s) for pre/post balancer usage.");
    return reports;
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch(InterruptedException e) {
      LOG.error("{}", e);
    }
  }
  
  private static Configuration createConf() {
    final Configuration conf = new HdfsConfiguration();
    TestBalancer.initConf(conf);
    return conf;
  }

  /**
   * First start a cluster and fill the cluster up to a certain size. Then
   * redistribute blocks according the required distribution. Finally, balance
   * the cluster.
   *
   * @param nNameNodes Number of NameNodes
   * @param nNameNodesToBalance Number of NameNodes to run the balancer on
   * @param distributionPerNN The distribution for each NameNode.
   * @param capacities Capacities of the datanodes
   * @param racks Rack names
   * @param conf Configuration
   */
  private void unevenDistribution(final int nNameNodes,
      final int nNameNodesToBalance, long distributionPerNN[],
      long capacities[], String[] racks, Configuration conf) throws Exception {
    LOG.info("UNEVEN 0");
    final int nDataNodes = distributionPerNN.length;
    if (capacities.length != nDataNodes || racks.length != nDataNodes) {
      throw new IllegalArgumentException("Array length is not the same");
    }

    if (nNameNodesToBalance > nNameNodes) {
      throw new IllegalArgumentException("Number of namenodes to balance is "
          + "greater than the number of namenodes.");
    }

    // calculate total space that need to be filled
    final long usedSpacePerNN = TestBalancer.sum(distributionPerNN);

    // fill the cluster
    final ExtendedBlock[][] blocks;
    {
      LOG.info("UNEVEN 1");
      final MiniDFSCluster cluster = new MiniDFSCluster
          .Builder(new Configuration(conf))
              .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(nNameNodes))
          .numDataNodes(nDataNodes)
          .racks(racks)
          .simulatedCapacities(capacities)
          .build();
      LOG.info("UNEVEN 2");
      try {
        cluster.waitActive();
        DFSTestUtil.setFederatedConfiguration(cluster, conf);
        LOG.info("UNEVEN 3");
        final Suite s = new Suite(cluster, nNameNodes, nDataNodes, null, conf);
        blocks = generateBlocks(s, usedSpacePerNN);
        LOG.info("UNEVEN 4");
      } finally {
        cluster.shutdown();
      }
    }

    conf.set(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, "0.0f");
    // Adjust the capacity of each DN since it will redistribute blocks
    // nNameNodes times in the following operations.
    long[] newCapacities = new long[nDataNodes];
    for (int i = 0; i < nDataNodes; i++) {
      newCapacities[i] = capacities[i] * nNameNodes;
    }
    {
      LOG.info("UNEVEN 10");
      final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(nNameNodes))
          .numDataNodes(nDataNodes)
          .racks(racks)
          .simulatedCapacities(newCapacities)
          .format(false)
          .build();
      LOG.info("UNEVEN 11");
      try {
        cluster.waitActive();
        LOG.info("UNEVEN 12");
        Set<String> blockpools = new HashSet<String>();
        for (int i = 0; i < nNameNodesToBalance; i++) {
          blockpools.add(cluster.getNamesystem(i).getBlockPoolId());
        }
        BalancerParameters.Builder b =
            new BalancerParameters.Builder();
        b.setBlockpools(blockpools);
        BalancerParameters params = b.build();
        final Suite s =
            new Suite(cluster, nNameNodes, nDataNodes, params, conf);
        for(int n = 0; n < nNameNodes; n++) {
          // redistribute blocks
          final Block[][] blocksDN = TestBalancer.distributeBlocks(
              blocks[n], s.replication, distributionPerNN);
    
          for(int d = 0; d < blocksDN.length; d++)
            cluster.injectBlocks(n, d, Arrays.asList(blocksDN[d]));

          LOG.info("UNEVEN 13: n=" + n);
        }
    
        final long totalCapacity = TestBalancer.sum(newCapacities);
        final long totalUsed = nNameNodes*usedSpacePerNN;
        LOG.info("UNEVEN 14");
        runBalancer(s, totalUsed, totalCapacity);
        LOG.info("UNEVEN 15");
      } finally {
        cluster.shutdown();
      }
      LOG.info("UNEVEN 16");
    }
  }


  /**
   * This test start a cluster, fill the DataNodes to be 30% full;
   * It then adds an empty node and start balancing.
   *
   * @param nNameNodes Number of NameNodes
   * @param racks Rack names
   * @param newRack the rack for the new DataNode
   * @param conf Configuration
   * @param nNameNodestoBalance noOfNameNodestoBalance
   * @param balancerParameters BalancerParameters
   */ 
  private void runTest(final int nNameNodes, String[] racks,
                       String[] newRack, Configuration conf,
                       int nNameNodestoBalance,
                       BalancerParameters balancerParameters)
      throws Exception {
    final int nDataNodes = racks.length;
    final long[] capacities = new long[nDataNodes];
    Arrays.fill(capacities, CAPACITY);
    LOG.info("nNameNodes=" + nNameNodes + ", nDataNodes=" + nDataNodes);
    Assert.assertEquals(nDataNodes, racks.length);

    LOG.info("RUN_TEST -1: start a cluster with nNameNodes=" + nNameNodes
        + ", nDataNodes=" + nDataNodes);

    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(new Configuration(conf))
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(nNameNodes))
        .numDataNodes(nDataNodes)
        .racks(racks)
        .simulatedCapacities(capacities)
        .build();
    LOG.info("RUN_TEST 0");
    DFSTestUtil.setFederatedConfiguration(cluster, conf);

    try {
      cluster.waitActive();
      LOG.info("RUN_TEST 1");

      Suite s;

      Set<String> blockpools = new HashSet<>();
      if(balancerParameters == null) {
        s = new Suite(cluster, nNameNodes, nDataNodes,
            BalancerParameters.DEFAULT, conf);

      } else {
        for (int i=0; i< nNameNodestoBalance; i++) {
          blockpools.add(cluster.getNamesystem(i).getBlockPoolId());
        }
        BalancerParameters.Builder b =
            new BalancerParameters.Builder();
        b.setBalancingPolicy(balancerParameters.getBalancingPolicy());
        b.setBlockpools(blockpools);
        BalancerParameters params = b.build();
        s = new Suite(cluster, nNameNodes, nDataNodes, params, conf, (short)2);
      }
      long totalCapacity = TestBalancer.sum(capacities);

      LOG.info("RUN_TEST 2: create files");
      // fill up the cluster to be 30% full
      final long totalUsed = totalCapacity * 3 / 10;
      final long size = (totalUsed/nNameNodes)/s.replication;
      for(int n = 0; n < nNameNodes; n++) {
        createFile(s, n, size);
      }

      LOG.info("RUN_TEST 3: " + newRack.length + " new datanodes");
      // start up an empty node with the same capacity and on the same rack
      final long[] newCapacity = new long[newRack.length];
      Arrays.fill(newCapacity, CAPACITY);
      cluster.startDataNodes(conf, newCapacity.length, true, null,
          newRack, newCapacity);

      totalCapacity += TestBalancer.sum(newCapacity);

      LOG.info("RUN_TEST 4: run Balancer");
      // run RUN_TEST and validate results
      runBalancer(s, totalUsed, totalCapacity);
      LOG.info("RUN_TEST 5");
    } finally {
      cluster.shutdown();
    }
    LOG.info("RUN_TEST 6: done");
  }

  
  /** Test a cluster with even distribution, 
   * then a new empty node is added to the cluster.
   */
  @Test
  public void testTwoOneOne() throws Exception {
    final Configuration conf = createConf();
    runTest(2, new String[]{RACK0}, new String[] {RACK0}, conf,
        2, null);
  }

  /** Test unevenly distributed cluster */
  @Test
  public void testUnevenDistribution() throws Exception {
    final Configuration conf = createConf();
    unevenDistribution(2, 2,
        new long[] {30*CAPACITY/100, 5*CAPACITY/100},
        new long[]{CAPACITY, CAPACITY},
        new String[] {RACK0, RACK1},
        conf);
  }

  @Test
  public void testBalancing1OutOf2Blockpools() throws Exception {
    final Configuration conf = createConf();
    unevenDistribution(2, 1, new long[] { 30 * CAPACITY / 100,
        5 * CAPACITY / 100 }, new long[] { CAPACITY, CAPACITY }, new String[] {
        RACK0, RACK1 }, conf);
  }

  @Test
  public void testBalancing2OutOf3Blockpools() throws Exception {
    final Configuration conf = createConf();
    unevenDistribution(3, 2, new long[] { 30 * CAPACITY / 100,
        5 * CAPACITY / 100, 10 * CAPACITY / 100 }, new long[] { CAPACITY,
        CAPACITY, CAPACITY }, new String[] { RACK0, RACK1, RACK2 }, conf);
  }

  /** Even distribution with 2 Namenodes, 4 Datanodes and 2 new Datanodes. */
  @Test(timeout = 600000)
  public void testTwoFourTwo() throws Exception {
    final Configuration conf = createConf();
    runTest(2, new String[]{RACK0, RACK0, RACK1, RACK1},
        new String[]{RACK2, RACK2}, conf, 2, null);
  }

  @Test(timeout=600000)
  public void testBalancingBlockpoolsWithBlockPoolPolicy() throws Exception {
    final Configuration conf = createConf();
    BalancerParameters balancerParameters = new BalancerParameters.Builder()
        .setBalancingPolicy(BalancingPolicy.Pool.INSTANCE).build();
    runTest(2, new String[]{RACK0, RACK0, RACK1, RACK1},
        new String[]{RACK2, RACK2}, conf, 2,
        balancerParameters);
  }

  @Test(timeout = 600000)
  public void test1OutOf2BlockpoolsWithBlockPoolPolicy()
      throws
      Exception {
    final Configuration conf = createConf();
    BalancerParameters balancerParameters = new BalancerParameters.Builder()
        .setBalancingPolicy(BalancingPolicy.Pool.INSTANCE).build();
    runTest(2, new String[]{RACK0, RACK0, RACK1, RACK1},
        new String[]{RACK2, RACK2}, conf, 1,
        balancerParameters);
  }


}
