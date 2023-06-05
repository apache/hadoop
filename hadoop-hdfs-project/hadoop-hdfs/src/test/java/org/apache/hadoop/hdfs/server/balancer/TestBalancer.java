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

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BALANCER_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BALANCER_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BALANCER_KEYTAB_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BALANCER_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;

import java.lang.reflect.Field;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.AfterClass;

import static org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset.CONFIG_PROPERTY_NONDFSUSED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.balancer.Balancer.Cli;
import org.apache.hadoop.hdfs.server.balancer.Balancer.Result;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.slf4j.event.Level;
import org.junit.After;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This class tests if a balancer schedules tasks correctly.
 */
public class TestBalancer {
  private static final Logger LOG = LoggerFactory.getLogger(TestBalancer.class);

  static {
    GenericTestUtils.setLogLevel(Balancer.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(Dispatcher.LOG, Level.DEBUG);
  }

  final static long CAPACITY = 5000L;
  final static String RACK0 = "/rack0";
  final static String RACK1 = "/rack1";
  final static String RACK2 = "/rack2";
  final private static String fileName = "/tmp.txt";
  final static Path filePath = new Path(fileName);
  final static private String username = "balancer";
  private static String principal;
  private static File baseDir;
  private static String keystoresDir;
  private static String sslConfDir;
  private static MiniKdc kdc;
  private static File keytabFile;
  private MiniDFSCluster cluster;
  private AtomicInteger numGetBlocksCalls;
  private AtomicLong startGetBlocksTime;
  private AtomicLong endGetBlocksTime;

  @Before
  public void setup() {
    numGetBlocksCalls = new AtomicInteger(0);
    startGetBlocksTime = new AtomicLong(Long.MAX_VALUE);
    endGetBlocksTime = new AtomicLong(Long.MIN_VALUE);
  }

  @After
  public void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  ClientProtocol client;

  static final long TIMEOUT = 40000L; //msec
  static final double CAPACITY_ALLOWED_VARIANCE = 0.005;  // 0.5%
  static final double BALANCE_ALLOWED_VARIANCE = 0.11;    // 10%+delta
  static final int DEFAULT_BLOCK_SIZE = 100;
  private static final Random r = new Random();

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
    conf.setInt(DFSConfigKeys.DFS_BALANCER_MAX_NO_MOVE_INTERVAL_KEY, 5*1000);
  }

  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final int dataBlocks = ecPolicy.getNumDataUnits();
  private final int parityBlocks = ecPolicy.getNumParityUnits();
  private final int groupSize = dataBlocks + parityBlocks;
  private final int cellSize = ecPolicy.getCellSize();
  private final int stripesPerBlock = 4;
  private final int defaultBlockSize = cellSize * stripesPerBlock;

  void initConfWithStripe(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, defaultBlockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    SimulatedFSDataset.setFactory(conf);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 1L);
  }

  static void initSecureConf(Configuration conf) throws Exception {
    baseDir = GenericTestUtils.getTestDir(TestBalancer.class.getSimpleName());
    FileUtil.fullyDelete(baseDir);
    assertTrue(baseDir.mkdirs());

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    KerberosName.resetDefaultRealm();
    assertTrue("Expected configuration to enable security",
        UserGroupInformation.isSecurityEnabled());

    keytabFile = new File(baseDir, username + ".keytab");
    String keytab = keytabFile.getAbsolutePath();
    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";
    principal = username + "/" + krbInstance + "@" + kdc.getRealm();
    String spnegoPrincipal = "HTTP/" + krbInstance + "@" + kdc.getRealm();
    kdc.createPrincipal(keytabFile, username, username + "/" + krbInstance,
        "HTTP/" + krbInstance);

    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, principal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, principal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    conf.setBoolean(DFS_BALANCER_KEYTAB_ENABLED_KEY, true);
    conf.set(DFS_BALANCER_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_BALANCER_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_BALANCER_KERBEROS_PRINCIPAL_KEY, principal);

    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestBalancer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);

    conf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
    initConf(conf);
  }

  @AfterClass
  public static void destroy() throws Exception {
    if (kdc != null) {
      kdc.stop();
      FileUtil.fullyDelete(baseDir);
      KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
    }
  }

  /* create a file with a length of <code>fileLen</code> */
  public static void createFile(MiniDFSCluster cluster, Path filePath, long
      fileLen,
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
             : Time.monotonicNow() + timeout;

    while (true) {
      long[] status = client.getStats();
      double totalSpaceVariance = Math.abs((double)status[0] - expectedTotalSpace)
          / expectedTotalSpace;
      double usedSpaceVariance = Math.abs((double)status[1] - expectedUsedSpace)
          / expectedUsedSpace;
      if (totalSpaceVariance < CAPACITY_ALLOWED_VARIANCE
          && usedSpaceVariance < CAPACITY_ALLOWED_VARIANCE)
        break; //done

      if (Time.monotonicNow() > failtime) {
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
      ClientProtocol client, MiniDFSCluster cluster, BalancerParameters p)
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
      ClientProtocol client, MiniDFSCluster cluster, BalancerParameters p,
      int expectedExcludedNodes) throws IOException, TimeoutException {
    waitForBalancer(totalUsedSpace, totalCapacity, client, cluster, p, expectedExcludedNodes, true);
  }

  /**
   * Wait until balanced: each datanode gives utilization within.
   * BALANCE_ALLOWED_VARIANCE of average
   * @throws IOException
   * @throws TimeoutException
   */
  static void waitForBalancer(long totalUsedSpace, long totalCapacity,
      ClientProtocol client, MiniDFSCluster cluster, BalancerParameters p,
      int expectedExcludedNodes, boolean checkExcludeNodesUtilization)
      throws IOException, TimeoutException {
    long timeout = TIMEOUT;
    long failtime = (timeout <= 0L) ? Long.MAX_VALUE
        : Time.monotonicNow() + timeout;
    if (!p.getIncludedNodes().isEmpty()) {
      totalCapacity = p.getIncludedNodes().size() * CAPACITY;
    }
    if (!p.getExcludedNodes().isEmpty()) {
      totalCapacity -= p.getExcludedNodes().size() * CAPACITY;
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
        double nodeUtilization =
            ((double) datanode.getDfsUsed() + datanode.getNonDfsUsed()) /
                datanode.getCapacity();
        if (Dispatcher.Util.isExcluded(p.getExcludedNodes(), datanode)) {
          if (checkExcludeNodesUtilization) {
            assertTrue(nodeUtilization == 0);
          }
          actualExcludedNodeCount++;
          continue;
        }
        if (!Dispatcher.Util.isIncluded(p.getIncludedNodes(), datanode)) {
          assertTrue(nodeUtilization == 0);
          actualExcludedNodeCount++;
          continue;
        }
        if (Math.abs(avgUtilization - nodeUtilization) > BALANCE_ALLOWED_VARIANCE) {
          balanced = false;
          if (Time.monotonicNow() > failtime) {
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

  private void doTest(Configuration conf, long[] capacities, String[] racks,
      long newCapacity, String newRack, NewNodeInfo nodes,
      boolean useTool, boolean useFile) throws Exception {
    doTest(conf, capacities, racks, newCapacity, 0L, newRack, nodes,
        useTool, useFile, false, 0.3);
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
   * @param useNamesystemSpy - spy on FSNamesystem if true
   * @param clusterUtilization - The utilization of the cluster to start, from
   *                             0.0 to 1.0
   * @throws Exception
   */
  private void doTest(Configuration conf, long[] capacities,
      String[] racks, long newCapacity, long newNonDfsUsed, String newRack,
      NewNodeInfo nodes, boolean useTool, boolean useFile,
      boolean useNamesystemSpy, double clusterUtilization) throws Exception {
    LOG.info("capacities = " +  long2String(capacities));
    LOG.info("racks      = " +  Arrays.asList(racks));
    LOG.info("newCapacity= " +  newCapacity);
    LOG.info("newRack    = " +  newRack);
    LOG.info("useTool    = " +  useTool);
    assertEquals(capacities.length, racks.length);
    int numOfDatanodes = capacities.length;

    try {
      cluster = new MiniDFSCluster
          .Builder(conf)
          .numDataNodes(0)
          .setNNRedundancyConsiderLoad(false)
          .build();
      cluster.getConfiguration(0).setInt(DFSConfigKeys.DFS_REPLICATION_KEY,
          DFSConfigKeys.DFS_REPLICATION_DEFAULT);
      conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY,
          DFSConfigKeys.DFS_REPLICATION_DEFAULT);
      if(useNamesystemSpy) {
        LOG.info("Using Spy Namesystem");
        spyFSNamesystem(cluster.getNameNode());
      }
      cluster.startDataNodes(conf, numOfDatanodes, true,
          StartupOption.REGULAR, racks, null, capacities, false);
      cluster.waitClusterUp();
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf,
          cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();

      long totalCapacity = sum(capacities);

      // fill up the cluster to be `clusterUtilization` full
      long totalDfsUsedSpace = (long) (totalCapacity * clusterUtilization);
      createFile(cluster, filePath, totalDfsUsedSpace / numOfDatanodes,
          (short) numOfDatanodes, 0);

      conf.setLong(CONFIG_PROPERTY_NONDFSUSED, newNonDfsUsed);
      if (nodes == null) { // there is no specification of new nodes.
        // start up an empty node with the same capacity and on the same rack
        cluster.startDataNodes(conf, 1, true, null,
            new String[]{newRack}, null,new long[]{newCapacity});
        totalCapacity += newCapacity;
        cluster.triggerHeartbeats();
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
          cluster.triggerHeartbeats();
          totalCapacity += newCapacity * nodes.getNumberofNewNodes();
        } else {  // host names are not specified
          cluster.startDataNodes(conf, nodes.getNumberofNewNodes(), true, null,
              newRacks, null, newCapacities);
          cluster.triggerHeartbeats();
          totalCapacity += newCapacity * nodes.getNumberofNewNodes();
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
      BalancerParameters.Builder pBuilder =
          new BalancerParameters.Builder();
      if (nodes != null) {
        pBuilder.setExcludedNodes(nodes.getNodesToBeExcluded());
        pBuilder.setIncludedNodes(nodes.getNodesToBeIncluded());
        pBuilder.setRunDuringUpgrade(false);
      }
      BalancerParameters p = pBuilder.build();

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
        runBalancerCli(conf, totalDfsUsedSpace, newNonDfsUsed,
            totalCapacity, p, useFile, expectedExcludedNodes);
      } else {
        runBalancer(conf, totalDfsUsedSpace, newNonDfsUsed,
            totalCapacity, p, expectedExcludedNodes, true);
      }
    } finally {
      if(cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void runBalancer(Configuration conf, long totalUsedSpace,
      long totalCapacity) throws Exception {
    runBalancer(conf, totalUsedSpace, totalCapacity,
        BalancerParameters.DEFAULT, 0);
  }

  private void runBalancer(Configuration conf, long totalDfsUsedSpace,
      long totalCapacity, BalancerParameters p, int excludedNodes)
      throws Exception {
    runBalancer(conf, totalDfsUsedSpace, 0, totalCapacity, p, excludedNodes,
        true);
  }

  private void runBalancer(Configuration conf, long totalDfsUsedSpace,
      long totalNonDfsUsedSpace, long totalCapacity, BalancerParameters p,
      int excludedNodes, boolean checkExcludeNodesUtilization)
      throws Exception {
    waitForHeartBeat(totalDfsUsedSpace, totalCapacity, client, cluster);

    int retry = 5;
    while (retry > 0) {
      // start rebalancing
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      final int run = runBalancer(namenodes, p, conf);
      if (conf.getInt(
          DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
          DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT)
          == 0) {
        assertEquals(ExitStatus.NO_MOVE_PROGRESS.getExitCode(), run);
        return;
      } else {
        assertEquals(ExitStatus.SUCCESS.getExitCode(), run);
      }
      waitForHeartBeat(totalDfsUsedSpace, totalCapacity, client, cluster);
      LOG.info("  .");
      try {
        long totalUsedSpace = totalDfsUsedSpace + totalNonDfsUsedSpace;
        waitForBalancer(totalUsedSpace, totalCapacity, client, cluster, p,
            excludedNodes, checkExcludeNodesUtilization);
      } catch (TimeoutException e) {
        // See HDFS-11682. NN may not get heartbeat to reflect the newest
        // block changes.
        retry--;
        if (retry == 0) {
          throw e;
        }
        LOG.warn("The cluster has not balanced yet, retry...");
        continue;
      }
      break;
    }
  }

  private static int runBalancer(Collection<URI> namenodes,
      final BalancerParameters p,
      Configuration conf) throws IOException, InterruptedException {
    final long sleeptime = conf.getLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 2000
        + conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
            DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_DEFAULT)
            * 1000;
    LOG.info("namenodes  = " + namenodes);
    LOG.info("parameters = " + p);
    LOG.info("Print stack trace", new Throwable());

    System.out.println("Time Stamp               Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved");

    List<NameNodeConnector> connectors = Collections.emptyList();
    try {
      connectors = NameNodeConnector.newNameNodeConnectors(namenodes,
          Balancer.class.getSimpleName(), Balancer.BALANCER_ID_PATH, conf,
              BalancerParameters.DEFAULT.getMaxIdleIteration());

      boolean done = false;
      for(int iteration = 0; !done; iteration++) {
        done = true;
        Collections.shuffle(connectors);
        for(NameNodeConnector nnc : connectors) {
          final Balancer b = new Balancer(nnc, p, conf);
          final Result r = b.runOneIteration();
          r.print(iteration, nnc, System.out);

          // clean all lists
          b.resetData(conf);
          if (r.getExitStatus() == ExitStatus.IN_PROGRESS) {
            done = false;
          } else if (r.getExitStatus() != ExitStatus.SUCCESS) {
            //must be an error statue, return.
            return r.getExitStatus().getExitCode();
          } else {
            if (iteration > 0) {
              assertTrue(r.getBytesAlreadyMoved() > 0);
            }
          }
        }

        if (!done) {
          Thread.sleep(sleeptime);
        }
      }
    } finally {
      for(NameNodeConnector nnc : connectors) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  private void runBalancerCli(Configuration conf, long totalDfsUsedSpace,
      long totalNonDfsUsedSpace, long totalCapacity, BalancerParameters p,
      boolean useFile, int expectedExcludedNodes) throws Exception {
    waitForHeartBeat(totalDfsUsedSpace, totalCapacity, client, cluster);
    List <String> args = new ArrayList<String>();
    args.add("-policy");
    args.add("datanode");

    File excludeHostsFile = null;
    if (!p.getExcludedNodes().isEmpty()) {
      args.add("-exclude");
      if (useFile) {
        excludeHostsFile = GenericTestUtils.getTestDir("exclude-hosts-file");
        PrintWriter pw = new PrintWriter(excludeHostsFile);
        for (String host : p.getExcludedNodes()) {
          pw.write( host + "\n");
        }
        pw.close();
        args.add("-f");
        args.add(excludeHostsFile.getAbsolutePath());
      } else {
        args.add(StringUtils.join(p.getExcludedNodes(), ','));
      }
    }

    File includeHostsFile = null;
    if (!p.getIncludedNodes().isEmpty()) {
      args.add("-include");
      if (useFile) {
        includeHostsFile = GenericTestUtils.getTestDir("include-hosts-file");
        PrintWriter pw = new PrintWriter(includeHostsFile);
        for (String host : p.getIncludedNodes()) {
          pw.write( host + "\n");
        }
        pw.close();
        args.add("-f");
        args.add(includeHostsFile.getAbsolutePath());
      } else {
        args.add(StringUtils.join(p.getIncludedNodes(), ','));
      }
    }

    final Tool tool = new Cli();
    tool.setConf(conf);
    final int r = tool.run(args.toArray(new String[0])); // start rebalancing

    assertEquals("Tools should exit 0 on success", 0, r);
    waitForHeartBeat(totalDfsUsedSpace, totalCapacity, client, cluster);
    LOG.info("Rebalancing with default ctor.");
    long totalUsedSpace = totalDfsUsedSpace + totalNonDfsUsedSpace;
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

  @Test(timeout = 100000)
  public void testUnknownDatanodeSimple() throws Exception {
    Configuration conf = new HdfsConfiguration();
    initConf(conf);
    testUnknownDatanode(conf);
  }

  /* we first start a cluster and fill the cluster up to a certain size.
   * then redistribute blocks according the required distribution.
   * Then we start an empty datanode.
   * Afterwards a balancer is run to balance the cluster.
   * A partially filled datanode is excluded during balancing.
   * This triggers a situation where one of the block's location is unknown.
   */
  private void testUnknownDatanode(Configuration conf)
      throws IOException, InterruptedException, TimeoutException {
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
    cluster.waitActive();
    client = NameNodeProxies.createProxy(conf, cluster.getFileSystem(0).getUri(),
        ClientProtocol.class).getProxy();

    for(int i = 0; i < 3; i++) {
      cluster.injectBlocks(i, Arrays.asList(blocksDN[i]), null);
    }

    cluster.startDataNodes(conf, 1, true, null,
        new String[]{RACK0}, null,new long[]{CAPACITY});
    cluster.triggerHeartbeats();

    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    Set<String>  datanodes = new HashSet<String>();
    datanodes.add(cluster.getDataNodes().get(0).getDatanodeId().getHostName());
    BalancerParameters.Builder pBuilder =
        new BalancerParameters.Builder();
    pBuilder.setExcludedNodes(datanodes);
    pBuilder.setRunDuringUpgrade(false);
    final int r = Balancer.run(namenodes, pBuilder.build(), conf);
    assertEquals(ExitStatus.SUCCESS.getExitCode(), r);
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
        new long[]{50 * CAPACITY / 100, 10 * CAPACITY / 100},
        new long[]{CAPACITY, CAPACITY},
        new String[]{RACK0, RACK1});
  }

  @Test(expected=HadoopIllegalArgumentException.class)
  public void testBalancerWithZeroThreadsForMove() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY, 0);
    testBalancer1Internal (conf);
  }

  @Test(timeout=100000)
  public void testBalancerWithNonZeroThreadsForMove() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY, 8);
    testBalancer1Internal(conf);
  }

  @Test(timeout=100000)
  public void testBalancer2() throws Exception {
    testBalancer2Internal(new HdfsConfiguration());
  }

  void testBalancer2Internal(Configuration conf) throws Exception {
    initConf(conf);
    testBalancerDefaultConstructor(conf, new long[]{CAPACITY, CAPACITY},
        new String[]{RACK0, RACK1}, CAPACITY, RACK2);
  }

  /** Test a cluster with even distribution,
   * then a new node with nonDfsUsed is added to the cluster. */
  @Test(timeout=100000)
  public void testBalancer3() throws Exception {
    Configuration conf = new HdfsConfiguration();
    initConf(conf);
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1},
        CAPACITY, 1000L, RACK2, null, false, false, false, 0.3);
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

    parameters = new String[] { "-blockpools" };
    try {
      Balancer.Cli.parse(parameters);
      fail("IllegalArgumentException is expected when a value "
          + "is not specified for the blockpool flag");
    } catch (IllegalArgumentException e) {

    }

    parameters = new String[] {"-source"};
    try {
      Balancer.Cli.parse(parameters);
      fail(reason + " for -source parameter");
    } catch (IllegalArgumentException ignored) {
      // expected
    }
  }

  @Test
  public void testBalancerCliParseBlockpools() {
    String[] parameters = new String[] { "-blockpools", "bp-1,bp-2,bp-3" };
    BalancerParameters p = Balancer.Cli.parse(parameters);
    assertEquals(3, p.getBlockPools().size());

    parameters = new String[] { "-blockpools", "bp-1" };
    p = Balancer.Cli.parse(parameters);
    assertEquals(1, p.getBlockPools().size());

    parameters = new String[] { "-blockpools", "bp-1,,bp-2" };
    p = Balancer.Cli.parse(parameters);
    assertEquals(3, p.getBlockPools().size());

    parameters = new String[] { "-blockpools", "bp-1," };
    p = Balancer.Cli.parse(parameters);
    assertEquals(1, p.getBlockPools().size());
  }

  @Test
  public void testBalancerCliParseHotBlockTimeInterval() {
    String[] parameters = new String[]{"-hotBlockTimeInterval", "1000"};
    BalancerParameters p = Balancer.Cli.parse(parameters);
    assertEquals(1000, p.getHotBlockTimeInterval());
  }

  @Test
  public void testBalancerDispatchHotBlockTimeInterval() {
    String[] parameters = new String[]{"-hotBlockTimeInterval", "1000"};
    BalancerParameters p = Balancer.Cli.parse(parameters);
    Configuration conf = new HdfsConfiguration();
    initConf(conf);
    try {
      cluster = new MiniDFSCluster
          .Builder(conf)
          .numDataNodes(0)
          .setNNRedundancyConsiderLoad(false)
          .build();
      cluster.getConfiguration(0).setInt(DFSConfigKeys.DFS_REPLICATION_KEY,
          DFSConfigKeys.DFS_REPLICATION_DEFAULT);
      conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY,
          DFSConfigKeys.DFS_REPLICATION_DEFAULT);
      cluster.waitClusterUp();
      cluster.waitActive();
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      List<NameNodeConnector> connectors =
          NameNodeConnector.newNameNodeConnectors(namenodes,
              Balancer.class.getSimpleName(),
              Balancer.BALANCER_ID_PATH, conf,
              BalancerParameters.DEFAULT.getMaxIdleIteration());
      Balancer run = new Balancer(
          connectors.get(0), p, new HdfsConfiguration());
      Field field = run.getClass().getDeclaredField("dispatcher");
      field.setAccessible(true);
      Object dispatcher = field.get(run);
      Field field1 =
          dispatcher.getClass().getDeclaredField("hotBlockTimeInterval");
      field1.setAccessible(true);
      Object hotBlockTimeInterval = field1.get(dispatcher);
      assertEquals(1000, (long)hotBlockTimeInterval);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
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
            excludeHosts, BalancerParameters.DEFAULT.getIncludedNodes()),
        false, false);
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
    doTest(conf, new long[] { CAPACITY, CAPACITY },
        new String[] { RACK0, RACK1 }, CAPACITY, RACK2, new HostNameBasedNodes(
            new String[] { "datanodeX", "datanodeY", "datanodeZ" },
            excludeHosts, BalancerParameters.DEFAULT.getIncludedNodes()), true,
        false);
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
            excludeHosts, BalancerParameters.DEFAULT.getIncludedNodes()), true,
        true);
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
            BalancerParameters.DEFAULT.getExcludedNodes(), includeHosts),
        false, false);
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
            BalancerParameters.DEFAULT.getExcludedNodes(), includeHosts), true,
        false);
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
            BalancerParameters.DEFAULT.getExcludedNodes(), includeHosts), true,
        true);
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
   * Check that the balancer exits when there is an unfinalized upgrade.
   */
  @Test(timeout=300000)
  public void testBalancerDuringUpgrade() throws Exception {
    final int SEED = 0xFADED;
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);

    conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 1L);

    final int BLOCK_SIZE = 1024*1024;
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(1)
        .simulatedCapacities(new long[]{BLOCK_SIZE * 10})
        .storageTypes(new StorageType[] { DEFAULT })
        .storagesPerDatanode(1)
        .build();
    cluster.waitActive();
    // Create a file on the single DN
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

    DistributedFileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, path1, BLOCK_SIZE, BLOCK_SIZE * 3, BLOCK_SIZE,
        (short) 1, SEED);

    // Add another DN with the same capacity, cluster is now unbalanced
    cluster.startDataNodes(conf, 1, true, null, null, null,
        new long[]{BLOCK_SIZE * 10}, false);
    cluster.triggerHeartbeats();
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);

    // Run balancer
    final BalancerParameters p = BalancerParameters.DEFAULT;

    fs.setSafeMode(SafeModeAction.ENTER);
    fs.rollingUpgrade(HdfsConstants.RollingUpgradeAction.PREPARE);
    fs.setSafeMode(SafeModeAction.LEAVE);

    // Rolling upgrade should abort the balancer
    assertEquals(ExitStatus.UNFINALIZED_UPGRADE.getExitCode(),
        Balancer.run(namenodes, p, conf));

    // Should work with the -runDuringUpgrade flag.
    BalancerParameters.Builder b =
        new BalancerParameters.Builder();
    b.setRunDuringUpgrade(true);
    final BalancerParameters runDuringUpgrade = b.build();
    assertEquals(ExitStatus.SUCCESS.getExitCode(),
        Balancer.run(namenodes, runDuringUpgrade, conf));

    // Finalize the rolling upgrade
    fs.rollingUpgrade(HdfsConstants.RollingUpgradeAction.FINALIZE);

    // Should also work after finalization.
    assertEquals(ExitStatus.SUCCESS.getExitCode(),
        Balancer.run(namenodes, p, conf));
  }

  /**
   * Test running many balancer simultaneously.
   *
   * Case-1: First balancer is running. Now, running second one should get
   * "Another balancer is running. Exiting.." IOException and fail immediately
   *
   * Case-2: When running second balancer 'balancer.id' file exists but the
   * lease doesn't exists. Now, the second balancer should run successfully.
   */
  @Test(timeout = 100000)
  public void testManyBalancerSimultaneously() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    // add an empty node with half of the capacities(4 * CAPACITY) & the same
    // rack
    long[] capacities = new long[] { 4 * CAPACITY };
    String[] racks = new String[] { RACK0 };
    long newCapacity = 2 * CAPACITY;
    String newRack = RACK0;
    LOG.info("capacities = " + long2String(capacities));
    LOG.info("racks      = " + Arrays.asList(racks));
    LOG.info("newCapacity= " + newCapacity);
    LOG.info("newRack    = " + newRack);
    LOG.info("useTool    = " + false);
    assertEquals(capacities.length, racks.length);
    int numOfDatanodes = capacities.length;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(capacities.length)
        .racks(racks).simulatedCapacities(capacities).build();
    cluster.waitActive();
    client = NameNodeProxies.createProxy(conf,
        cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();

    long totalCapacity = sum(capacities);

    // fill up the cluster to be 30% full
    final long totalUsedSpace = totalCapacity * 3 / 10;
    createFile(cluster, filePath, totalUsedSpace / numOfDatanodes,
        (short) numOfDatanodes, 0);
    // start up an empty node with the same capacity and on the same rack
    cluster.startDataNodes(conf, 1, true, null, new String[] { newRack },
        new long[] { newCapacity });
    cluster.triggerHeartbeats();

    // Case1: Simulate first balancer by creating 'balancer.id' file. It
    // will keep this file until the balancing operation is completed.
    FileSystem fs = cluster.getFileSystem(0);
    final FSDataOutputStream out = fs
        .create(Balancer.BALANCER_ID_PATH, false);
    out.writeBytes(InetAddress.getLocalHost().getHostName());
    out.hflush();
    assertTrue("'balancer.id' file doesn't exist!",
        fs.exists(Balancer.BALANCER_ID_PATH));

    // start second balancer
    final String[] args = { "-policy", "datanode" };
    final Tool tool = new Cli();
    tool.setConf(conf);
    int exitCode = tool.run(args); // start balancing
    assertEquals("Exit status code mismatches",
        ExitStatus.IO_EXCEPTION.getExitCode(), exitCode);

    // Case2: Release lease so that another balancer would be able to
    // perform balancing.
    out.close();
    assertTrue("'balancer.id' file doesn't exist!",
        fs.exists(Balancer.BALANCER_ID_PATH));
    exitCode = tool.run(args); // start balancing
    assertEquals("Exit status code mismatches",
        ExitStatus.SUCCESS.getExitCode(), exitCode);
  }

  public void integrationTestWithStripedFile(Configuration conf) throws Exception {
    initConfWithStripe(conf);
    doTestBalancerWithStripedFile(conf);
  }

  @Test(timeout = 200000)
  public void testBalancerWithStripedFile() throws Exception {
    Configuration conf = new Configuration();
    initConfWithStripe(conf);
    NameNodeConnector.setWrite2IdFile(true);
    doTestBalancerWithStripedFile(conf);
    NameNodeConnector.setWrite2IdFile(false);
  }

  private void doTestBalancerWithStripedFile(Configuration conf) throws Exception {
    int numOfDatanodes = dataBlocks + parityBlocks + 3;
    int numOfRacks = dataBlocks;
    long capacity = 20 * defaultBlockSize;
    long[] capacities = new long[numOfDatanodes];
    for (int i = 0; i < capacities.length; i++) {
      capacities[i] = capacity;
    }
    String[] racks = new String[numOfDatanodes];
    for (int i = 0; i < numOfDatanodes; i++) {
      racks[i] = "/rack" + (i % numOfRacks);
    }
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numOfDatanodes)
        .racks(racks)
        .simulatedCapacities(capacities)
        .build();

    try {
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf, cluster.getFileSystem(0).getUri(),
          ClientProtocol.class).getProxy();
      client.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());
      client.setErasureCodingPolicy("/",
          StripedFileTestUtil.getDefaultECPolicy().getName());

      long totalCapacity = sum(capacities);

      // fill up the cluster with 30% data. It'll be 45% full plus parity.
      long fileLen = totalCapacity * 3 / 10;
      long totalUsedSpace = fileLen * (dataBlocks + parityBlocks) / dataBlocks;
      FileSystem fs = cluster.getFileSystem(0);
      DFSTestUtil.createFile(fs, filePath, fileLen, (short) 3, r.nextLong());

      // verify locations of striped blocks
      LocatedBlocks locatedBlocks = client.getBlockLocations(fileName, 0, fileLen);
      StripedFileTestUtil.verifyLocatedStripedBlocks(locatedBlocks, groupSize);

      // add datanodes in new rack
      String newRack = "/rack" + (++numOfRacks);
      cluster.startDataNodes(conf, 2, true, null,
          new String[]{newRack, newRack}, null,
          new long[]{capacity, capacity});
      totalCapacity += capacity*2;
      cluster.triggerHeartbeats();

      // run balancer and validate results
      BalancerParameters p = BalancerParameters.DEFAULT;
      runBalancer(conf, totalUsedSpace, totalCapacity, p, 0);

      // namenode will ask datanode to delete replicas in heartbeat response
      cluster.triggerHeartbeats();
      // namenode will update block locations according to the report
      cluster.triggerDeletionReports();

      // verify locations of striped blocks
      locatedBlocks = client.getBlockLocations(fileName, 0, fileLen);
      StripedFileTestUtil.verifyLocatedStripedBlocks(locatedBlocks, groupSize);

      // Test handling NPE with striped blocks
      testNullStripedBlocks(conf);

    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testBalancerWithExcludeListWithStripedFile() throws Exception {
    Configuration conf = new Configuration();
    initConfWithStripe(conf);
    NameNodeConnector.setWrite2IdFile(true);
    doTestBalancerWithExcludeListWithStripedFile(conf);
    NameNodeConnector.setWrite2IdFile(false);
  }

  private void doTestBalancerWithExcludeListWithStripedFile(Configuration conf) throws Exception {
    int numOfDatanodes = dataBlocks + parityBlocks + 5;
    int numOfRacks = dataBlocks;
    long capacity = 20 * defaultBlockSize;
    long[] capacities = new long[numOfDatanodes];
    Arrays.fill(capacities, capacity);
    String[] racks = new String[numOfDatanodes];
    for (int i = 0; i < numOfDatanodes; i++) {
      racks[i] = "/rack" + (i % numOfRacks);
    }
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numOfDatanodes)
        .racks(racks)
        .simulatedCapacities(capacities)
        .build();

    try {
      cluster.waitActive();
      client = NameNodeProxies.createProxy(conf, cluster.getFileSystem(0).getUri(),
          ClientProtocol.class).getProxy();
      client.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());
      client.setErasureCodingPolicy("/",
          StripedFileTestUtil.getDefaultECPolicy().getName());

      long totalCapacity = sum(capacities);

      // fill up the cluster with 30% data. It'll be 45% full plus parity.
      long fileLen = totalCapacity * 3 / 10;
      long totalUsedSpace = fileLen * (dataBlocks + parityBlocks) / dataBlocks;
      FileSystem fs = cluster.getFileSystem(0);
      DFSTestUtil.createFile(fs, filePath, fileLen, (short) 3, r.nextLong());

      // verify locations of striped blocks
      LocatedBlocks locatedBlocks = client.getBlockLocations(fileName, 0, fileLen);
      StripedFileTestUtil.verifyLocatedStripedBlocks(locatedBlocks, groupSize);

      // get datanode report
      DatanodeInfo[] datanodeReport = client.getDatanodeReport(DatanodeReportType.ALL);
      long totalBlocks = 0;
      for (DatanodeInfo dn : datanodeReport) {
        totalBlocks += dn.getNumBlocks();
      }

      // add datanode in new rack
      String newRack = "/rack" + (++numOfRacks);
      cluster.startDataNodes(conf, 2, true, null,
          new String[]{newRack, newRack}, null,
          new long[]{capacity, capacity});
      totalCapacity += capacity*2;
      cluster.triggerHeartbeats();

      // add datanode to exclude list
      Set<String> excludedList = new HashSet<>();
      excludedList.add(datanodeReport[0].getXferAddr());
      BalancerParameters.Builder pBuilder = new BalancerParameters.Builder();
      pBuilder.setExcludedNodes(excludedList);

      // start balancer and check the failed num of moving task
      runBalancer(conf, totalUsedSpace, 0, totalCapacity, pBuilder.build(),
          excludedList.size(), false);

      // check total blocks, max wait time 60s
      final long blocksBeforeBalancer = totalBlocks;
      GenericTestUtils.waitFor(() -> {
        DatanodeInfo[] datanodeInfos = null;
        try {
          cluster.triggerHeartbeats();
          datanodeInfos = client.getDatanodeReport(DatanodeReportType.ALL);
        } catch (IOException e) {
          Assert.fail(e.getMessage());
        }
        long blocksAfterBalancer = 0;
        for (DatanodeInfo dn : datanodeInfos) {
          blocksAfterBalancer += dn.getNumBlocks();
        }
        return blocksBeforeBalancer == blocksAfterBalancer;
      }, 3000, 60000);

      // verify locations of striped blocks
      locatedBlocks = client.getBlockLocations(fileName, 0, fileLen);
      StripedFileTestUtil.verifyLocatedStripedBlocks(locatedBlocks, groupSize);

    } finally {
      cluster.shutdown();
    }
  }

  private void testNullStripedBlocks(Configuration conf) throws IOException {
    NameNodeConnector nnc = NameNodeConnector.newNameNodeConnectors(
        DFSUtil.getInternalNsRpcUris(conf),
        Balancer.class.getSimpleName(), Balancer.BALANCER_ID_PATH, conf,
        BalancerParameters.DEFAULT.getMaxIdleIteration()).get(0);
    Dispatcher dispatcher = new Dispatcher(nnc, Collections.emptySet(),
        Collections.<String> emptySet(), 1, 1, 0,
        1, 1, conf);
    Dispatcher spyDispatcher = spy(dispatcher);
    Dispatcher.PendingMove move = spyDispatcher.new PendingMove(
        mock(Dispatcher.Source.class),
        mock(Dispatcher.DDatanode.StorageGroup.class));
    Dispatcher.DBlockStriped block = mock(Dispatcher.DBlockStriped.class);

    doReturn(null).when(block).getInternalBlock(any());
    doReturn(true)
        .when(spyDispatcher)
        .isGoodBlockCandidate(any(), any(), any(), any());

    when(move.markMovedIfGoodBlock(block, DEFAULT)).thenCallRealMethod();

    assertFalse(move.markMovedIfGoodBlock(block, DEFAULT));
  }

  /**
   * Test Balancer runs fine when logging in with a keytab in kerberized env.
   * Reusing testUnknownDatanode here for basic functionality testing.
   */
  @Test(timeout = 300000)
  public void testBalancerWithKeytabs() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    try {
      initSecureConf(conf);
      final UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          principal, keytabFile.getAbsolutePath());
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          // verify that balancer runs Ok.
          testUnknownDatanode(conf);
          // verify that UGI was logged in using keytab.
          assertTrue(UserGroupInformation.isLoginKeytabBased());
          return null;
        }
      });
    } finally {
      // Reset UGI so that other tests are not affected.
      UserGroupInformation.reset();
      UserGroupInformation.setConfiguration(new Configuration());
    }
  }

  private void spyFSNamesystem(NameNode nn) throws IOException {
    FSNamesystem fsnSpy = NameNodeAdapter.spyOnNamesystem(nn);
    doAnswer(new Answer<BlocksWithLocations>() {
      @Override
      public BlocksWithLocations answer(InvocationOnMock invocation)
          throws Throwable {
        long startTime = Time.monotonicNow();
        startGetBlocksTime.getAndUpdate((curr) -> Math.min(curr, startTime));
        BlocksWithLocations blk =
            (BlocksWithLocations)invocation.callRealMethod();
        long endTime = Time.monotonicNow();
        endGetBlocksTime.getAndUpdate((curr) -> Math.max(curr, endTime));
        numGetBlocksCalls.incrementAndGet();
        return blk;
      }}).when(fsnSpy).getBlocks(any(DatanodeID.class),
        anyLong(), anyLong(), anyLong());
  }

  /**
   * Test that makes the Balancer to disperse RPCs to the NameNode
   * in order to avoid NN's RPC queue saturation. This not marked as @Test
   * because it is run from {@link TestBalancerRPCDelay}.
   */
  void testBalancerRPCDelay(int getBlocksMaxQps) throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    conf.setInt(DFSConfigKeys.DFS_BALANCER_DISPATCHERTHREADS_KEY, 30);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_MAX_QPS_KEY,
        getBlocksMaxQps);

    int numDNs = 20;
    long[] capacities = new long[numDNs];
    String[] racks = new String[numDNs];
    for(int i = 0; i < numDNs; i++) {
      capacities[i] = CAPACITY;
      racks[i] = (i < numDNs/2 ? RACK0 : RACK1);
    }
    doTest(conf, capacities, racks, CAPACITY, 0L, RACK2,
        // Use only 1 node and set the starting capacity to 50% to allow the
        // balancing to complete in only one iteration. This is necessary
        // because the startGetBlocksTime and endGetBlocksTime measures across
        // all get block calls, so if two iterations are performed, the duration
        // also includes the time it took to perform the block move ops in the
        // first iteration
        new PortNumberBasedNodes(1, 0, 0), false, false, true, 0.5);
    assertTrue("Number of getBlocks should be not less than " +
        getBlocksMaxQps, numGetBlocksCalls.get() >= getBlocksMaxQps);
    long durationMs = 1 + endGetBlocksTime.get() - startGetBlocksTime.get();
    int durationSec = (int) Math.ceil(durationMs / 1000.0);
    LOG.info("Balancer executed {} getBlocks in {} msec (round up to {} sec)",
        numGetBlocksCalls.get(), durationMs, durationSec);
    long getBlockCallsPerSecond = numGetBlocksCalls.get() / durationSec;
    assertTrue("Expected balancer getBlocks calls per second <= " +
        getBlocksMaxQps, getBlockCallsPerSecond <= getBlocksMaxQps);
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
