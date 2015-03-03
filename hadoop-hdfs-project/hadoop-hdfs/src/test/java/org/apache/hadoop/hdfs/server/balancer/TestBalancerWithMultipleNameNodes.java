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
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
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
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test balancer with multiple NameNodes
 */
public class TestBalancerWithMultipleNameNodes {
  static final Log LOG = Balancer.LOG;
  {
    ((Log4JLogger)LOG).getLogger().setLevel(Level.ALL);
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
  }

  
  private static final long CAPACITY = 500L;
  private static final String RACK0 = "/rack0";
  private static final String RACK1 = "/rack1";

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
    
    Suite(MiniDFSCluster cluster, final int nNameNodes, final int nDataNodes,
        Configuration conf) throws IOException {
      this.conf = conf;
      this.cluster = cluster;
      clients = new ClientProtocol[nNameNodes];
      for(int i = 0; i < nNameNodes; i++) {
        clients[i] = cluster.getNameNode(i).getRpcServer();
      }
      replication = (short)Math.max(1, nDataNodes - 1);
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
      final long fileLen = size/s.replication;
      createFile(s, n, fileLen);

      final List<LocatedBlock> locatedBlocks = s.clients[n].getBlockLocations(
          FILE_NAME, 0, fileLen).getLocatedBlocks();

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
  static void wait(final ClientProtocol[] clients,
      long expectedUsedSpace, long expectedTotalSpace) throws IOException {
    LOG.info("WAIT expectedUsedSpace=" + expectedUsedSpace
        + ", expectedTotalSpace=" + expectedTotalSpace);
    for(int n = 0; n < clients.length; n++) {
      int i = 0;
      for(boolean done = false; !done; ) {
        final long[] s = clients[n].getStats();
        done = s[0] == expectedTotalSpace && s[1] == expectedUsedSpace;
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
    final double avg = totalUsed*100.0/totalCapacity;

    LOG.info("BALANCER 0: totalUsed=" + totalUsed
        + ", totalCapacity=" + totalCapacity
        + ", avg=" + avg);
    wait(s.clients, totalUsed, totalCapacity);
    LOG.info("BALANCER 1");

    // start rebalancing
    final Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(s.conf);
    final int r = Balancer.run(namenodes, Balancer.Parameters.DEFAULT, s.conf);
    Assert.assertEquals(ExitStatus.SUCCESS.getExitCode(), r);

    LOG.info("BALANCER 2");
    wait(s.clients, totalUsed, totalCapacity);
    LOG.info("BALANCER 3");

    int i = 0;
    for(boolean balanced = false; !balanced; i++) {
      final long[] used = new long[s.cluster.getDataNodes().size()];
      final long[] cap = new long[used.length];

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
        }
      }

      balanced = true;
      for(int d = 0; d < used.length; d++) {
        final double p = used[d]*100.0/cap[d];
        balanced = p <= avg + Balancer.Parameters.DEFAULT.threshold;
        if (!balanced) {
          if (i % 100 == 0) {
            LOG.warn("datanodes " + d + " is not yet balanced: "
                + "used=" + used[d] + ", cap=" + cap[d] + ", avg=" + avg);
            LOG.warn("TestBalancer.sum(used)=" + TestBalancer.sum(used)
                + ", TestBalancer.sum(cap)=" + TestBalancer.sum(cap));
          }
          sleep(100);
          break;
        }
      }
    }
    LOG.info("BALANCER 6");
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch(InterruptedException e) {
      LOG.error(e);
    }
  }
  
  private static Configuration createConf() {
    final Configuration conf = new HdfsConfiguration();
    TestBalancer.initConf(conf);
    return conf;
  }

  /**
   * First start a cluster and fill the cluster up to a certain size.
   * Then redistribute blocks according the required distribution.
   * Finally, balance the cluster.
   * 
   * @param nNameNodes Number of NameNodes
   * @param distributionPerNN The distribution for each NameNode. 
   * @param capacities Capacities of the datanodes
   * @param racks Rack names
   * @param conf Configuration
   */
  private void unevenDistribution(final int nNameNodes,
      long distributionPerNN[], long capacities[], String[] racks,
      Configuration conf) throws Exception {
    LOG.info("UNEVEN 0");
    final int nDataNodes = distributionPerNN.length;
    if (capacities.length != nDataNodes || racks.length != nDataNodes) {
      throw new IllegalArgumentException("Array length is not the same");
    }

    // calculate total space that need to be filled
    final long usedSpacePerNN = TestBalancer.sum(distributionPerNN);

    // fill the cluster
    final ExtendedBlock[][] blocks;
    {
      LOG.info("UNEVEN 1");
      final MiniDFSCluster cluster = new MiniDFSCluster
          .Builder(new Configuration(conf))
          .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
          .numDataNodes(nDataNodes)
          .racks(racks)
          .simulatedCapacities(capacities)
          .build();
      LOG.info("UNEVEN 2");
      try {
        cluster.waitActive();
        DFSTestUtil.setFederatedConfiguration(cluster, conf);
        LOG.info("UNEVEN 3");
        final Suite s = new Suite(cluster, nNameNodes, nDataNodes, conf);
        blocks = generateBlocks(s, usedSpacePerNN);
        LOG.info("UNEVEN 4");
      } finally {
        cluster.shutdown();
      }
    }

    conf.set(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, "0.0f");
    {
      LOG.info("UNEVEN 10");
      final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(nNameNodes))
          .numDataNodes(nDataNodes)
          .racks(racks)
          .simulatedCapacities(capacities)
          .format(false)
          .build();
      LOG.info("UNEVEN 11");
      try {
        cluster.waitActive();
        LOG.info("UNEVEN 12");
        final Suite s = new Suite(cluster, nNameNodes, nDataNodes, conf);
        for(int n = 0; n < nNameNodes; n++) {
          // redistribute blocks
          final Block[][] blocksDN = TestBalancer.distributeBlocks(
              blocks[n], s.replication, distributionPerNN);
    
          for(int d = 0; d < blocksDN.length; d++)
            cluster.injectBlocks(n, d, Arrays.asList(blocksDN[d]));

          LOG.info("UNEVEN 13: n=" + n);
        }
    
        final long totalCapacity = TestBalancer.sum(capacities);
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
   * @param capacities Capacities of the datanodes
   * @param racks Rack names
   * @param newCapacity the capacity of the new DataNode
   * @param newRack the rack for the new DataNode
   * @param conf Configuration
   */ 
  private void runTest(final int nNameNodes, long[] capacities, String[] racks,
      long newCapacity, String newRack, Configuration conf) throws Exception {
    final int nDataNodes = capacities.length;
    LOG.info("nNameNodes=" + nNameNodes + ", nDataNodes=" + nDataNodes);
    Assert.assertEquals(nDataNodes, racks.length);

    LOG.info("RUN_TEST -1");
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
      final Suite s = new Suite(cluster, nNameNodes, nDataNodes, conf);
      long totalCapacity = TestBalancer.sum(capacities);

      LOG.info("RUN_TEST 2");
      // fill up the cluster to be 30% full
      final long totalUsed = totalCapacity*3/10;
      final long size = (totalUsed/nNameNodes)/s.replication;
      for(int n = 0; n < nNameNodes; n++) {
        createFile(s, n, size);
      }

      LOG.info("RUN_TEST 3");
      // start up an empty node with the same capacity and on the same rack
      cluster.startDataNodes(conf, 1, true, null,
          new String[]{newRack}, new long[]{newCapacity});

      totalCapacity += newCapacity;

      LOG.info("RUN_TEST 4");
      // run RUN_TEST and validate results
      runBalancer(s, totalUsed, totalCapacity);
      LOG.info("RUN_TEST 5");
    } finally {
      cluster.shutdown();
    }
    LOG.info("RUN_TEST 6");
  }
  
  /** Test a cluster with even distribution, 
   * then a new empty node is added to the cluster
   */
  @Test
  public void testBalancer() throws Exception {
    final Configuration conf = createConf();
    runTest(2, new long[]{CAPACITY}, new String[]{RACK0},
        CAPACITY/2, RACK0, conf);
  }

  /** Test unevenly distributed cluster */
  @Test
  public void testUnevenDistribution() throws Exception {
    final Configuration conf = createConf();
    unevenDistribution(2,
        new long[] {30*CAPACITY/100, 5*CAPACITY/100},
        new long[]{CAPACITY, CAPACITY},
        new String[] {RACK0, RACK1},
        conf);
  }
}
