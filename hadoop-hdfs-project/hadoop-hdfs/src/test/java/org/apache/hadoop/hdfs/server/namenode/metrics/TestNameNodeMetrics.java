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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeAdapter;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for metrics published by the Namenode
 */
public class TestNameNodeMetrics {
  private static final Configuration CONF = new HdfsConfiguration();
  private static final int DFS_REPLICATION_INTERVAL = 1;
  private static final Path TEST_ROOT_DIR_PATH = 
    new Path("/testNameNodeMetrics");
  private static final String NN_METRICS = "NameNodeActivity";
  private static final String NS_METRICS = "FSNamesystem";
  
  // Number of datanodes in the cluster
  private static final int DATANODE_COUNT = 3; 
  static {
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 100);
    CONF.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
    CONF.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFS_REPLICATION_INTERVAL);
    CONF.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 
        DFS_REPLICATION_INTERVAL);

    ((Log4JLogger)LogFactory.getLog(MetricsAsserts.class))
      .getLogger().setLevel(Level.DEBUG);
  }
  
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Random rand = new Random();
  private FSNamesystem namesystem;
  private BlockManager bm;

  private static Path getTestPath(String fileName) {
    return new Path(TEST_ROOT_DIR_PATH, fileName);
  }
  
  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(DATANODE_COUNT).build();
    cluster.waitActive();
    namesystem = cluster.getNamesystem();
    bm = namesystem.getBlockManager();
    fs = (DistributedFileSystem) cluster.getFileSystem();
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }
  
  /** create a file with a length of <code>fileLen</code> */
  private void createFile(Path file, long fileLen, short replicas) throws IOException {
    DFSTestUtil.createFile(fs, file, fileLen, replicas, rand.nextLong());
  }

  private void updateMetrics() throws Exception {
    // Wait for metrics update (corresponds to dfs.namenode.replication.interval
    // for some block related metrics to get updated)
    Thread.sleep(1000);
  }

  private void readFile(FileSystem fileSys,Path name) throws IOException {
    //Just read file so that getNumBlockLocations are incremented
    DataInputStream stm = fileSys.open(name);
    byte [] buffer = new byte[4];
    stm.read(buffer,0,4);
    stm.close();
  }
  
  /** Test metrics associated with addition of a file */
  @Test
  public void testFileAdd() throws Exception {
    // Add files with 100 blocks
    final Path file = getTestPath("testFileAdd");
    createFile(file, 3200, (short)3);
    final long blockCount = 32;
    int blockCapacity = namesystem.getBlockCapacity();
    updateMetrics();
    assertGauge("BlockCapacity", blockCapacity, getMetrics(NS_METRICS));

    MetricsRecordBuilder rb = getMetrics(NN_METRICS);
    // File create operations is 1
    // Number of files created is depth of <code>file</code> path
    assertCounter("CreateFileOps", 1L, rb);
    assertCounter("FilesCreated", (long)file.depth(), rb);

    // Blocks are stored in a hashmap. Compute its capacity, which
    // doubles every time the number of entries reach the threshold.
    int threshold = (int)(blockCapacity * BlockManager.DEFAULT_MAP_LOAD_FACTOR);
    while (threshold < blockCount) {
      blockCapacity <<= 1;
    }
    updateMetrics();
    long filesTotal = file.depth() + 1; // Add 1 for root
    rb = getMetrics(NS_METRICS);
    assertGauge("FilesTotal", filesTotal, rb);
    assertGauge("BlocksTotal", blockCount, rb);
    assertGauge("BlockCapacity", blockCapacity, rb);
    fs.delete(file, true);
    filesTotal--; // reduce the filecount for deleted file
    
    waitForDeletion();
    updateMetrics();
    rb = getMetrics(NS_METRICS);
    assertGauge("FilesTotal", filesTotal, rb);
    assertGauge("BlocksTotal", 0L, rb);
    assertGauge("PendingDeletionBlocks", 0L, rb);

    rb = getMetrics(NN_METRICS);
    // Delete file operations and number of files deleted must be 1
    assertCounter("DeleteFileOps", 1L, rb);
    assertCounter("FilesDeleted", 1L, rb);
  }
  
  /** Corrupt a block and ensure metrics reflects it */
  @Test
  public void testCorruptBlock() throws Exception {
    // Create a file with single block with two replicas
    final Path file = getTestPath("testCorruptBlock");
    createFile(file, 100, (short)2);
    
    // Disable the heartbeats, so that no corrupted replica
    // can be fixed
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeAdapter.setHeartbeatsDisabledForTests(dn, true);
    }
    
    // Corrupt first replica of the block
    LocatedBlock block = NameNodeAdapter.getBlockLocations(
        cluster.getNameNode(), file.toString(), 0, 1).get(0);
    bm.findAndMarkBlockAsCorrupt(block.getBlock(), block.getLocations()[0],
        "TEST");
    updateMetrics();
    MetricsRecordBuilder rb = getMetrics(NS_METRICS);
    assertGauge("CorruptBlocks", 1L, rb);
    assertGauge("PendingReplicationBlocks", 1L, rb);
    assertGauge("ScheduledReplicationBlocks", 1L, rb);
    fs.delete(file, true);
    waitForDeletion();
    rb = getMetrics(NS_METRICS);
    assertGauge("CorruptBlocks", 0L, rb);
    assertGauge("PendingReplicationBlocks", 0L, rb);
    assertGauge("ScheduledReplicationBlocks", 0L, rb);
  }
  
  /** Create excess blocks by reducing the replication factor for
   * for a file and ensure metrics reflects it
   */
  @Test
  public void testExcessBlocks() throws Exception {
    Path file = getTestPath("testExcessBlocks");
    createFile(file, 100, (short)2);
    long totalBlocks = 1;
    NameNodeAdapter.setReplication(namesystem, file.toString(), (short)1);
    updateMetrics();
    MetricsRecordBuilder rb = getMetrics(NS_METRICS);
    assertGauge("ExcessBlocks", totalBlocks, rb);
    fs.delete(file, true);
  }
  
  /** Test to ensure metrics reflects missing blocks */
  @Test
  public void testMissingBlock() throws Exception {
    // Create a file with single block with two replicas
    Path file = getTestPath("testMissingBlocks");
    createFile(file, 100, (short)1);
    
    // Corrupt the only replica of the block to result in a missing block
    LocatedBlock block = NameNodeAdapter.getBlockLocations(
        cluster.getNameNode(), file.toString(), 0, 1).get(0);
    bm.findAndMarkBlockAsCorrupt(block.getBlock(), block.getLocations()[0],
        "TEST");
    updateMetrics();
    MetricsRecordBuilder rb = getMetrics(NS_METRICS);
    assertGauge("UnderReplicatedBlocks", 1L, rb);
    assertGauge("MissingBlocks", 1L, rb);
    fs.delete(file, true);
    waitForDeletion();
    assertGauge("UnderReplicatedBlocks", 0L, getMetrics(NS_METRICS));
  }

  private void waitForDeletion() throws InterruptedException {
    // Wait for more than DATANODE_COUNT replication intervals to ensure all
    // the blocks pending deletion are sent for deletion to the datanodes.
    Thread.sleep(DFS_REPLICATION_INTERVAL * (DATANODE_COUNT + 1) * 1000);
  }
  
  @Test
  public void testRenameMetrics() throws Exception {
    Path src = getTestPath("src");
    createFile(src, 100, (short)1);
    Path target = getTestPath("target");
    createFile(target, 100, (short)1);
    fs.rename(src, target, Rename.OVERWRITE);
    updateMetrics();
    MetricsRecordBuilder rb = getMetrics(NN_METRICS);
    assertCounter("FilesRenamed", 1L, rb);
    assertCounter("FilesDeleted", 1L, rb);
  }
  
  /**
   * Test numGetBlockLocations metric   
   * 
   * Test initiates and performs file operations (create,read,close,open file )
   * which results in metrics changes. These metrics changes are updated and 
   * tested for correctness.
   * 
   *  create file operation does not increment numGetBlockLocation
   *  one read file operation increments numGetBlockLocation by 1
   *    
   * @throws IOException in case of an error
   */
  @Test
  public void testGetBlockLocationMetric() throws Exception {
    Path file1_Path = new Path(TEST_ROOT_DIR_PATH, "file1.dat");

    // When cluster starts first time there are no file  (read,create,open)
    // operations so metric GetBlockLocations should be 0.
    assertCounter("GetBlockLocations", 0L, getMetrics(NN_METRICS));

    //Perform create file operation
    createFile(file1_Path,100,(short)2);
    updateMetrics();
  
    //Create file does not change numGetBlockLocations metric
    //expect numGetBlockLocations = 0 for previous and current interval 
    assertCounter("GetBlockLocations", 0L, getMetrics(NN_METRICS));
  
    // Open and read file operation increments GetBlockLocations
    // Perform read file operation on earlier created file
    readFile(fs, file1_Path);
    updateMetrics();
    // Verify read file operation has incremented numGetBlockLocations by 1
    assertCounter("GetBlockLocations", 1L, getMetrics(NN_METRICS));

    // opening and reading file  twice will increment numGetBlockLocations by 2
    readFile(fs, file1_Path);
    readFile(fs, file1_Path);
    updateMetrics();
    assertCounter("GetBlockLocations", 3L, getMetrics(NN_METRICS));
  }
  
  /**
   * Test NN checkpoint and transaction-related metrics.
   */
  @Test
  public void testTransactionAndCheckpointMetrics() throws Exception {
    long lastCkptTime = MetricsAsserts.getLongGauge("LastCheckpointTime",
        getMetrics(NS_METRICS));
    
    assertGauge("LastCheckpointTime", lastCkptTime, getMetrics(NS_METRICS));
    assertGauge("LastWrittenTransactionId", 1L, getMetrics(NS_METRICS));
    assertGauge("TransactionsSinceLastCheckpoint", 1L, getMetrics(NS_METRICS));
    assertGauge("TransactionsSinceLastLogRoll", 1L, getMetrics(NS_METRICS));
    
    fs.mkdirs(new Path(TEST_ROOT_DIR_PATH, "/tmp"));
    updateMetrics();
    
    assertGauge("LastCheckpointTime", lastCkptTime, getMetrics(NS_METRICS));
    assertGauge("LastWrittenTransactionId", 2L, getMetrics(NS_METRICS));
    assertGauge("TransactionsSinceLastCheckpoint", 2L, getMetrics(NS_METRICS));
    assertGauge("TransactionsSinceLastLogRoll", 2L, getMetrics(NS_METRICS));
    
    cluster.getNameNodeRpc().rollEditLog();
    updateMetrics();
    
    assertGauge("LastCheckpointTime", lastCkptTime, getMetrics(NS_METRICS));
    assertGauge("LastWrittenTransactionId", 4L, getMetrics(NS_METRICS));
    assertGauge("TransactionsSinceLastCheckpoint", 4L, getMetrics(NS_METRICS));
    assertGauge("TransactionsSinceLastLogRoll", 1L, getMetrics(NS_METRICS));
    
    cluster.getNameNodeRpc().setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    cluster.getNameNodeRpc().saveNamespace();
    cluster.getNameNodeRpc().setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    updateMetrics();
    
    long newLastCkptTime = MetricsAsserts.getLongGauge("LastCheckpointTime",
        getMetrics(NS_METRICS));
    assertTrue(lastCkptTime < newLastCkptTime);
    assertGauge("LastWrittenTransactionId", 6L, getMetrics(NS_METRICS));
    assertGauge("TransactionsSinceLastCheckpoint", 1L, getMetrics(NS_METRICS));
    assertGauge("TransactionsSinceLastLogRoll", 1L, getMetrics(NS_METRICS));
  }
}
