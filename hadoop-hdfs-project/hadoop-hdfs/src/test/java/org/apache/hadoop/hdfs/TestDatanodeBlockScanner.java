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

package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Test;

/**
 * This test verifies that block verification occurs on the datanode
 */
public class TestDatanodeBlockScanner {
  
  private static final Log LOG = 
                 LogFactory.getLog(TestDatanodeBlockScanner.class);
  
  private static final long TIMEOUT = 20000; // 20 sec.
  
  private static final Pattern pattern =
             Pattern.compile(".*?(blk_[-]*\\d+).*?scan time\\s*:\\s*(\\d+)");
  
  private static final Pattern pattern_blockVerify =
             Pattern.compile(".*?(SCAN_PERIOD)\\s*:\\s*(\\d+.*?)");
  
  static {
    ((Log4JLogger)FSNamesystem.auditLog).getLogger().setLevel(Level.WARN);
  }
  /**
   * This connects to datanode and fetches block verification data.
   * It repeats this until the given block has a verification time > newTime.
   * @param newTime - validation timestamps before newTime are "old", the
   *            result of previous validations.  This method waits until a "new"
   *            validation timestamp is obtained.  If no validator runs soon
   *            enough, the method will time out.
   * @return - the new validation timestamp
   * @throws IOException
   * @throws TimeoutException
   */
  private static long waitForVerification(int infoPort, FileSystem fs, 
                          Path file, int blocksValidated, 
                          long newTime, long timeout) 
  throws IOException, TimeoutException {
    URL url = new URL("http://localhost:" + infoPort +
                      "/blockScannerReport?listblocks");
    long lastWarnTime = Time.monotonicNow();
    if (newTime <= 0) newTime = 1L;
    long verificationTime = 0;
    
    String block = DFSTestUtil.getFirstBlock(fs, file).getBlockName();
    long failtime = (timeout <= 0) ? Long.MAX_VALUE 
        : Time.monotonicNow() + timeout;
    while (verificationTime < newTime) {
      if (failtime < Time.monotonicNow()) {
        throw new TimeoutException("failed to achieve block verification after "
            + timeout + " msec.  Current verification timestamp = "
            + verificationTime + ", requested verification time > " 
            + newTime);
      }
      String response = DFSTestUtil.urlGet(url);
      if(blocksValidated >= 0) {
        for(Matcher matcher = pattern_blockVerify.matcher(response); matcher.find();) {
          if (block.equals(matcher.group(1))) {
            assertEquals(1, blocksValidated);
            break;
          }
        }
      }
      for(Matcher matcher = pattern.matcher(response); matcher.find();) {
        if (block.equals(matcher.group(1))) {
          verificationTime = Long.parseLong(matcher.group(2));
          break;
        }
      }
      
      if (verificationTime < newTime) {
        long now = Time.monotonicNow();
        if ((now - lastWarnTime) >= 5*1000) {
          LOG.info("Waiting for verification of " + block);
          lastWarnTime = now; 
        }
        try {
          Thread.sleep(500);
        } catch (InterruptedException ignored) {}
      }
    }
    
    return verificationTime;
  }

  @Test
  public void testDatanodeBlockScanner() throws IOException, TimeoutException {
    long startTime = Time.monotonicNow();
    
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    
    FileSystem fs = cluster.getFileSystem();
    Path file1 = new Path("/tmp/testBlockVerification/file1");
    Path file2 = new Path("/tmp/testBlockVerification/file2");
    
    /*
     * Write the first file and restart the cluster.
     */
    DFSTestUtil.createFile(fs, file1, 10, (short)1, 0);
    cluster.shutdown();

    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(1)
                                .format(false).build();
    cluster.waitActive();
    
    DFSClient dfsClient =  new DFSClient(new InetSocketAddress("localhost", 
                                         cluster.getNameNodePort()), conf);
    fs = cluster.getFileSystem();
    DatanodeInfo dn = dfsClient.datanodeReport(DatanodeReportType.LIVE)[0];
    
    /*
     * The cluster restarted. The block should be verified by now.
     */
    assertTrue(waitForVerification(dn.getInfoPort(), fs, file1, 1, startTime,
        TIMEOUT) >= startTime);
    
    /*
     * Create a new file and read the block. The block should be marked 
     * verified since the client reads the block and verifies checksum. 
     */
    DFSTestUtil.createFile(fs, file2, 10, (short)1, 0);
    IOUtils.copyBytes(fs.open(file2), new IOUtils.NullOutputStream(), 
                      conf, true); 
    assertTrue(waitForVerification(dn.getInfoPort(), fs, file2, 2, startTime,
        TIMEOUT) >= startTime);
    
    cluster.shutdown();
  }

  public static boolean corruptReplica(ExtendedBlock blk, int replica) throws IOException {
    return MiniDFSCluster.corruptReplica(replica, blk);
  }

  @Test
  public void testBlockCorruptionPolicy() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000L);
    Random random = new Random();
    FileSystem fs = null;
    int rand = random.nextInt(3);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    Path file1 = new Path("/tmp/testBlockVerification/file1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)3, 0);
    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, file1);
    
    DFSTestUtil.waitReplication(fs, file1, (short)3);
    assertFalse(DFSTestUtil.allBlockReplicasCorrupt(cluster, file1, 0));

    // Corrupt random replica of block 
    assertTrue(MiniDFSCluster.corruptReplica(rand, block));

    // Restart the datanode hoping the corrupt block to be reported
    cluster.restartDataNode(rand);

    // We have 2 good replicas and block is not corrupt
    DFSTestUtil.waitReplication(fs, file1, (short)2);
    assertFalse(DFSTestUtil.allBlockReplicasCorrupt(cluster, file1, 0));
  
    // Corrupt all replicas. Now, block should be marked as corrupt
    // and we should get all the replicas 
    assertTrue(MiniDFSCluster.corruptReplica(0, block));
    assertTrue(MiniDFSCluster.corruptReplica(1, block));
    assertTrue(MiniDFSCluster.corruptReplica(2, block));

    // Trigger each of the DNs to scan this block immediately.
    // The block pool scanner doesn't run frequently enough on its own
    // to notice these, and due to HDFS-1371, the client won't report
    // bad blocks to the NN when all replicas are bad.
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.runBlockScannerForBlock(dn, block);
    }

    // We now have the blocks to be marked as corrupt and we get back all
    // its replicas
    DFSTestUtil.waitReplication(fs, file1, (short)3);
    assertTrue(DFSTestUtil.allBlockReplicasCorrupt(cluster, file1, 0));
    cluster.shutdown();
  }
  
  /**
   * testBlockCorruptionRecoveryPolicy.
   * This tests recovery of corrupt replicas, first for one corrupt replica
   * then for two. The test invokes blockCorruptionRecoveryPolicy which
   * 1. Creates a block with desired number of replicas
   * 2. Corrupts the desired number of replicas and restarts the datanodes
   *    containing the corrupt replica. Additionaly we also read the block
   *    in case restarting does not report corrupt replicas.
   *    Restarting or reading from the datanode would trigger reportBadBlocks 
   *    to namenode.
   *    NameNode adds it to corruptReplicasMap and neededReplication
   * 3. Test waits until all corrupt replicas are reported, meanwhile
   *    Re-replciation brings the block back to healthy state
   * 4. Test again waits until the block is reported with expected number
   *    of good replicas.
   */
  @Test
  public void testBlockCorruptionRecoveryPolicy1() throws Exception {
    // Test recovery of 1 corrupt replica
    LOG.info("Testing corrupt replica recovery for one corrupt replica");
    blockCorruptionRecoveryPolicy(4, (short)3, 1);
  }

  @Test
  public void testBlockCorruptionRecoveryPolicy2() throws Exception {
    // Test recovery of 2 corrupt replicas
    LOG.info("Testing corrupt replica recovery for two corrupt replicas");
    blockCorruptionRecoveryPolicy(5, (short)3, 2);
  }
  
  private void blockCorruptionRecoveryPolicy(int numDataNodes, 
                                             short numReplicas,
                                             int numCorruptReplicas) 
                                             throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 30L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 3);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 3L);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, false);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, 5L);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    Path file1 = new Path("/tmp/testBlockCorruptRecovery/file");
    DFSTestUtil.createFile(fs, file1, 1024, numReplicas, 0);
    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, file1);
    final int ITERATIONS = 10;

    // Wait until block is replicated to numReplicas
    DFSTestUtil.waitReplication(fs, file1, numReplicas);

    for (int k = 0; ; k++) {
      // Corrupt numCorruptReplicas replicas of block 
      int[] corruptReplicasDNIDs = new int[numCorruptReplicas];
      for (int i=0, j=0; (j != numCorruptReplicas) && (i < numDataNodes); i++) {
        if (corruptReplica(block, i)) {
          corruptReplicasDNIDs[j++] = i;
          LOG.info("successfully corrupted block " + block + " on node " 
                   + i + " " + cluster.getDataNodes().get(i).getDisplayName());
        }
      }
      
      // Restart the datanodes containing corrupt replicas 
      // so they would be reported to namenode and re-replicated
      // They MUST be restarted in reverse order from highest to lowest index,
      // because the act of restarting them removes them from the ArrayList
      // and causes the indexes of all nodes above them in the list to change.
      for (int i = numCorruptReplicas - 1; i >= 0 ; i--) {
        LOG.info("restarting node with corrupt replica: position " 
            + i + " node " + corruptReplicasDNIDs[i] + " " 
            + cluster.getDataNodes().get(corruptReplicasDNIDs[i]).getDisplayName());
        cluster.restartDataNode(corruptReplicasDNIDs[i]);
      }

      // Loop until all corrupt replicas are reported
      try {
        DFSTestUtil.waitCorruptReplicas(fs, cluster.getNamesystem(), file1, 
                                        block, numCorruptReplicas);
      } catch(TimeoutException e) {
        if (k > ITERATIONS) {
          throw e;
        }
        LOG.info("Timed out waiting for corrupt replicas, trying again, iteration " + k);
        continue;
      }
      break;
    }
    
    // Loop until the block recovers after replication
    DFSTestUtil.waitReplication(fs, file1, numReplicas);
    assertFalse(DFSTestUtil.allBlockReplicasCorrupt(cluster, file1, 0));

    // Make sure the corrupt replica is invalidated and removed from
    // corruptReplicasMap
    DFSTestUtil.waitCorruptReplicas(fs, cluster.getNamesystem(), file1, 
        block, 0);
    cluster.shutdown();
  }
  
  /** Test if NameNode handles truncated blocks in block report */
  @Test
  public void testTruncatedBlockReport() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final short REPLICATION_FACTOR = (short)2;
    final Path fileName = new Path("/file1");

    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 3L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 3);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 3L);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, false);

    long startTime = Time.monotonicNow();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(REPLICATION_FACTOR)
                                               .build();
    cluster.waitActive();
    
    ExtendedBlock block;
    try {
      FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, fileName, 1, REPLICATION_FACTOR, 0);
      DFSTestUtil.waitReplication(fs, fileName, REPLICATION_FACTOR);
      block = DFSTestUtil.getFirstBlock(fs, fileName);
    } finally {
      cluster.shutdown();
    }

    // Restart cluster and confirm block is verified on datanode 0,
    // then truncate it on datanode 0.
    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(REPLICATION_FACTOR)
                                .format(false)
                                .build();
    cluster.waitActive();
    try {
      FileSystem fs = cluster.getFileSystem();
      int infoPort = cluster.getDataNodes().get(0).getInfoPort();
      assertTrue(waitForVerification(infoPort, fs, fileName, 1, startTime, TIMEOUT) >= startTime);
      
      // Truncate replica of block
      if (!changeReplicaLength(block, 0, -1)) {
        throw new IOException(
            "failed to find or change length of replica on node 0 "
            + cluster.getDataNodes().get(0).getDisplayName());
      }      
    } finally {
      cluster.shutdown();
    }

    // Restart the cluster, add a node, and check that the truncated block is 
    // handled correctly
    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(REPLICATION_FACTOR)
                                .format(false)
                                .build();
    cluster.startDataNodes(conf, 1, true, null, null);
    cluster.waitActive();  // now we have 3 datanodes

    // Assure the cluster has left safe mode.
    cluster.waitClusterUp();
    assertFalse("failed to leave safe mode", 
        cluster.getNameNode().isInSafeMode());

    try {
      // wait for truncated block be detected by block scanner,
      // and the block to be replicated
      DFSTestUtil.waitReplication(
          cluster.getFileSystem(), fileName, REPLICATION_FACTOR);
      
      // Make sure that truncated block will be deleted
      waitForBlockDeleted(block, 0, TIMEOUT);
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Change the length of a block at datanode dnIndex
   */
  static boolean changeReplicaLength(ExtendedBlock blk, int dnIndex,
      int lenDelta) throws IOException {
    File blockFile = MiniDFSCluster.getBlockFile(dnIndex, blk);
    if (blockFile != null && blockFile.exists()) {
      RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
      raFile.setLength(raFile.length()+lenDelta);
      raFile.close();
      return true;
    }
    LOG.info("failed to change length of block " + blk);
    return false;
  }
  
  private static void waitForBlockDeleted(ExtendedBlock blk, int dnIndex,
      long timeout) throws TimeoutException, InterruptedException {
    File blockFile = MiniDFSCluster.getBlockFile(dnIndex, blk);
    long failtime = Time.monotonicNow()
                    + ((timeout > 0) ? timeout : Long.MAX_VALUE);
    while (blockFile != null && blockFile.exists()) {
      if (failtime < Time.monotonicNow()) {
        throw new TimeoutException("waited too long for blocks to be deleted: "
            + blockFile.getPath() + (blockFile.exists() ? " still exists; " : " is absent; "));
      }
      Thread.sleep(100);
      blockFile = MiniDFSCluster.getBlockFile(dnIndex, blk);
    }
  }
  
  private static final String BASE_PATH = (new File("/data/current/finalized"))
      .getAbsolutePath();
  
  @Test
  public void testReplicaInfoParsing() throws Exception {
    testReplicaInfoParsingSingle(BASE_PATH);
    testReplicaInfoParsingSingle(BASE_PATH + "/subdir1");
    testReplicaInfoParsingSingle(BASE_PATH + "/subdir1/subdir2/subdir3");
  }
  
  private static void testReplicaInfoParsingSingle(String subDirPath) {
    File testFile = new File(subDirPath);
    assertEquals(BASE_PATH, ReplicaInfo.parseBaseDir(testFile).baseDirPath);
  }

  @Test
  public void testDuplicateScans() throws Exception {
    long startTime = Time.monotonicNow();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration())
        .numDataNodes(1).build();
    FileSystem fs = null;
    try {
      fs = cluster.getFileSystem();
      DataNode dataNode = cluster.getDataNodes().get(0);
      int infoPort = dataNode.getInfoPort();
      long scanTimeBefore = 0, scanTimeAfter = 0;
      for (int i = 1; i < 10; i++) {
        Path fileName = new Path("/test" + i);
        DFSTestUtil.createFile(fs, fileName, 1024, (short) 1, 1000L);
        waitForVerification(infoPort, fs, fileName, i, startTime, TIMEOUT);
        if (i > 1) {
          scanTimeAfter = DataNodeTestUtils.getLatestScanTime(dataNode,
              DFSTestUtil.getFirstBlock(fs, new Path("/test" + (i - 1))));
          assertFalse("scan time shoud not be 0", scanTimeAfter == 0);
          assertEquals("There should not be duplicate scan", scanTimeBefore,
              scanTimeAfter);
        }

        scanTimeBefore = DataNodeTestUtils.getLatestScanTime(dataNode,
            DFSTestUtil.getFirstBlock(fs, new Path("/test" + i)));
      }
      cluster.restartDataNode(0);
      Thread.sleep(10000);
      dataNode = cluster.getDataNodes().get(0);
      scanTimeAfter = DataNodeTestUtils.getLatestScanTime(dataNode,
          DFSTestUtil.getFirstBlock(fs, new Path("/test" + (9))));
      assertEquals("There should not be duplicate scan", scanTimeBefore,
          scanTimeAfter);
    } finally {
      IOUtils.closeStream(fs);
      cluster.shutdown();
    }
  }
}
