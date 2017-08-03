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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.assertQuantileGauges;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Joiner;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
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
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.top.TopAuditLogger;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.hadoop.util.Time;
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
  private static final int WAIT_GAUGE_VALUE_RETRIES = 20;
  
  // Rollover interval of percentile metrics (in seconds)
  private static final int PERCENTILES_INTERVAL = 1;

  static {
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 100);
    CONF.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
    CONF.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFS_REPLICATION_INTERVAL);
    CONF.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 
        DFS_REPLICATION_INTERVAL);
    // Set it long enough to essentially disable unless we manually call it
    // Used for decommissioning DataNode metrics
    CONF.setInt(MiniDFSCluster.DFS_NAMENODE_DECOMMISSION_INTERVAL_TESTING_KEY,
        9999999);
    // For checking failed volume metrics
    CONF.setInt(DFSConfigKeys.DFS_DF_INTERVAL_KEY, 1000);
    CONF.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);
    CONF.set(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY, 
        "" + PERCENTILES_INTERVAL);
    // Enable stale DataNodes checking
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    ((Log4JLogger)LogFactory.getLog(MetricsAsserts.class))
      .getLogger().setLevel(Level.DEBUG);
  }
  
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private final Random rand = new Random();
  private FSNamesystem namesystem;
  private BlockManager bm;
  // List of temporary files on local FileSystem to be cleaned up
  private List<Path> tempFiles;

  private static Path getTestPath(String fileName) {
    return new Path(TEST_ROOT_DIR_PATH, fileName);
  }
  
  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(DATANODE_COUNT).build();
    cluster.waitActive();
    namesystem = cluster.getNamesystem();
    bm = namesystem.getBlockManager();
    fs = cluster.getFileSystem();
    tempFiles = new ArrayList<>();
  }
  
  @After
  public void tearDown() throws Exception {
    MetricsSource source = DefaultMetricsSystem.instance().getSource("UgiMetrics");
    if (source != null) {
      // Run only once since the UGI metrics is cleaned up during teardown
      MetricsRecordBuilder rb = getMetrics(source);
      assertQuantileGauges("GetGroups1s", rb);
    }
    cluster.shutdown();
    for (Path p : tempFiles) {
      FileUtils.deleteQuietly(new File(p.toUri().getPath()));
    }
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

  /**
   * Test that capacity metrics are exported and pass
   * basic sanity tests.
   */
  @Test (timeout = 1800)
  public void testCapacityMetrics() throws Exception {
    MetricsRecordBuilder rb = getMetrics(NS_METRICS);
    long capacityTotal = MetricsAsserts.getLongGauge("CapacityTotal", rb);
    assert(capacityTotal != 0);
    long capacityUsed = MetricsAsserts.getLongGauge("CapacityUsed", rb);
    long capacityRemaining =
        MetricsAsserts.getLongGauge("CapacityRemaining", rb);
    long capacityUsedNonDFS =
        MetricsAsserts.getLongGauge("CapacityUsedNonDFS", rb);
    // There will be 5% space reserved in ext filesystem which is not
    // considered.
    assert(capacityUsed + capacityRemaining + capacityUsedNonDFS <=
        capacityTotal);
  }

  /** Test metrics indicating the number of stale DataNodes */
  @Test
  public void testStaleNodes() throws Exception {
    // Set two datanodes as stale
    for (int i = 0; i < 2; i++) {
      DataNode dn = cluster.getDataNodes().get(i);
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
      long staleInterval = CONF.getLong(
          DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
          DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
      DatanodeDescriptor dnDes = cluster.getNameNode().getNamesystem()
          .getBlockManager().getDatanodeManager()
          .getDatanode(dn.getDatanodeId());
      DFSTestUtil.resetLastUpdatesWithOffset(dnDes, -(staleInterval + 1));
    }
    // Let HeartbeatManager to check heartbeat
    BlockManagerTestUtil.checkHeartbeat(cluster.getNameNode().getNamesystem()
        .getBlockManager());
    assertGauge("StaleDataNodes", 2, getMetrics(NS_METRICS));
    
    // Reset stale datanodes
    for (int i = 0; i < 2; i++) {
      DataNode dn = cluster.getDataNodes().get(i);
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, false);
      DatanodeDescriptor dnDes = cluster.getNameNode().getNamesystem()
          .getBlockManager().getDatanodeManager()
          .getDatanode(dn.getDatanodeId());
      DFSTestUtil.resetLastUpdatesWithOffset(dnDes, 0);
    }
    
    // Let HeartbeatManager to refresh
    BlockManagerTestUtil.checkHeartbeat(cluster.getNameNode().getNamesystem()
        .getBlockManager());
    assertGauge("StaleDataNodes", 0, getMetrics(NS_METRICS));
  }

  /**
   * Test metrics associated with volume failures.
   */
  @Test
  public void testVolumeFailures() throws Exception {
    assertGauge("VolumeFailuresTotal", 0, getMetrics(NS_METRICS));
    assertGauge("EstimatedCapacityLostTotal", 0L, getMetrics(NS_METRICS));
    DataNode dn = cluster.getDataNodes().get(0);
    FsVolumeSpi fsVolume =
        DataNodeTestUtils.getFSDataset(dn).getVolumes().get(0);
    File dataDir = new File(fsVolume.getBasePath());
    long capacity = ((FsVolumeImpl) fsVolume).getCapacity();
    DataNodeTestUtils.injectDataDirFailure(dataDir);
    long lastDiskErrorCheck = dn.getLastDiskErrorCheck();
    dn.checkDiskErrorAsync();
    while (dn.getLastDiskErrorCheck() == lastDiskErrorCheck) {
      Thread.sleep(100);
    }
    DataNodeTestUtils.triggerHeartbeat(dn);
    BlockManagerTestUtil.checkHeartbeat(bm);
    assertGauge("VolumeFailuresTotal", 1, getMetrics(NS_METRICS));
    assertGauge("EstimatedCapacityLostTotal", capacity, getMetrics(NS_METRICS));
  }

  /**
   * Test metrics associated with liveness and decommission status of DataNodes.
   */
  @Test
  public void testDataNodeLivenessAndDecom() throws Exception {
    Path hostFileDir = new Path(MiniDFSCluster.getBaseDirectory(), "hosts");
    FileSystem localFs = FileSystem.getLocal(CONF);
    localFs.mkdirs(hostFileDir);
    Path includeFile = new Path(hostFileDir, "include");
    Path excludeFile = new Path(hostFileDir, "exclude");
    tempFiles.add(includeFile);
    tempFiles.add(excludeFile);
    CONF.set(DFSConfigKeys.DFS_HOSTS, includeFile.toUri().getPath());
    CONF.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());

    List<DataNode> dataNodes = cluster.getDataNodes();
    DatanodeDescriptor[] dnDescriptors = new DatanodeDescriptor[DATANODE_COUNT];
    String[] dnAddresses = new String[DATANODE_COUNT];
    for (int i = 0; i < DATANODE_COUNT; i++) {
      dnDescriptors[i] = bm.getDatanodeManager()
          .getDatanode(dataNodes.get(i).getDatanodeId());
      dnAddresses[i] = dnDescriptors[i].getXferAddr();
    }
    // First put all DNs into include
    DFSTestUtil.writeFile(localFs, includeFile,
        Joiner.on("\n").join(dnAddresses));
    DFSTestUtil.writeFile(localFs, excludeFile, "");
    bm.getDatanodeManager().refreshNodes(CONF);
    assertGauge("NumDecomLiveDataNodes", 0, getMetrics(NS_METRICS));
    assertGauge("NumLiveDataNodes", DATANODE_COUNT, getMetrics(NS_METRICS));

    // Now decommission one DN
    DFSTestUtil.writeFile(localFs, excludeFile, dnAddresses[0]);
    bm.getDatanodeManager().refreshNodes(CONF);
    assertGauge("NumDecommissioningDataNodes", 1, getMetrics(NS_METRICS));
    BlockManagerTestUtil.recheckDecommissionState(bm.getDatanodeManager());
    assertGauge("NumDecommissioningDataNodes", 0, getMetrics(NS_METRICS));
    assertGauge("NumDecomLiveDataNodes", 1, getMetrics(NS_METRICS));
    assertGauge("NumLiveDataNodes", DATANODE_COUNT, getMetrics(NS_METRICS));

    // Now kill all DNs by expiring their heartbeats
    for (int i = 0; i < DATANODE_COUNT; i++) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dataNodes.get(i), true);
      long expireInterval = CONF.getLong(
          DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
          DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT) * 2L
          + CONF.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
          DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 10 * 1000L;
      DFSTestUtil.resetLastUpdatesWithOffset(dnDescriptors[i],
          -(expireInterval + 1));
    }
    BlockManagerTestUtil.checkHeartbeat(bm);
    assertGauge("NumDecomLiveDataNodes", 0, getMetrics(NS_METRICS));
    assertGauge("NumDecomDeadDataNodes", 1, getMetrics(NS_METRICS));
    assertGauge("NumLiveDataNodes", 0, getMetrics(NS_METRICS));
    assertGauge("NumDeadDataNodes", DATANODE_COUNT, getMetrics(NS_METRICS));

    // Now remove the decommissioned DN altogether
    String[] includeHosts = new String[dnAddresses.length - 1];
    for (int i = 0; i < includeHosts.length; i++) {
      includeHosts[i] = dnAddresses[i + 1];
    }
    DFSTestUtil.writeFile(localFs, includeFile,
        Joiner.on("\n").join(includeHosts));
    // Just init to a nonexistent host to clear out the previous exclusion
    DFSTestUtil.writeFile(localFs, excludeFile, "");
    bm.getDatanodeManager().refreshNodes(CONF);
    assertGauge("NumDecomLiveDataNodes", 0, getMetrics(NS_METRICS));
    assertGauge("NumDecomDeadDataNodes", 0, getMetrics(NS_METRICS));
    assertGauge("NumLiveDataNodes", 0, getMetrics(NS_METRICS));
    assertGauge("NumDeadDataNodes", DATANODE_COUNT - 1, getMetrics(NS_METRICS));

    // Finally mark the remaining DNs as live again
    for (int i = 1; i < dataNodes.size(); i++) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dataNodes.get(i), false);
      DFSTestUtil.resetLastUpdatesWithOffset(dnDescriptors[i], 0);
    }
    BlockManagerTestUtil.checkHeartbeat(bm);
    assertGauge("NumLiveDataNodes", DATANODE_COUNT - 1, getMetrics(NS_METRICS));
    assertGauge("NumDeadDataNodes", 0, getMetrics(NS_METRICS));
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

    updateMetrics();
    long filesTotal = file.depth() + 1; // Add 1 for root
    rb = getMetrics(NS_METRICS);
    assertGauge("FilesTotal", filesTotal, rb);
    assertGauge("BlocksTotal", blockCount, rb);
    fs.delete(file, true);
    filesTotal--; // reduce the filecount for deleted file

    rb = waitForDnMetricValue(NS_METRICS, "FilesTotal", filesTotal);
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
    
    // Corrupt first replica of the block
    LocatedBlock block = NameNodeAdapter.getBlockLocations(
        cluster.getNameNode(), file.toString(), 0, 1).get(0);
    cluster.getNamesystem().writeLock();
    try {
      bm.findAndMarkBlockAsCorrupt(block.getBlock(), block.getLocations()[0],
          "STORAGE_ID", "TEST");
    } finally {
      cluster.getNamesystem().writeUnlock();
    }
    updateMetrics();
    MetricsRecordBuilder rb = getMetrics(NS_METRICS);
    assertGauge("CorruptBlocks", 1L, rb);
    assertGauge("PendingReplicationBlocks", 1L, rb);
    assertGauge("ScheduledReplicationBlocks", 1L, rb);
    fs.delete(file, true);
    rb = waitForDnMetricValue(NS_METRICS, "CorruptBlocks", 0L);
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
    NameNodeAdapter.setReplication(namesystem, file.toString(), (short)1);
    updateMetrics();
    MetricsRecordBuilder rb = getMetrics(NS_METRICS);
    assertGauge("ExcessBlocks", 1L, rb);

    // verify ExcessBlocks metric is decremented and
    // excessReplicateMap is cleared after deleting a file
    fs.delete(file, true);
    rb = getMetrics(NS_METRICS);
    assertGauge("ExcessBlocks", 0L, rb);
    assertTrue(bm.excessReplicateMap.isEmpty());
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
    cluster.getNamesystem().writeLock();
    try {
      bm.findAndMarkBlockAsCorrupt(block.getBlock(), block.getLocations()[0],
          "STORAGE_ID", "TEST");
    } finally {
      cluster.getNamesystem().writeUnlock();
    }
    updateMetrics();
    MetricsRecordBuilder rb = getMetrics(NS_METRICS);
    assertGauge("UnderReplicatedBlocks", 1L, rb);
    assertGauge("MissingBlocks", 1L, rb);
    assertGauge("MissingReplOneBlocks", 1L, rb);
    fs.delete(file, true);
    waitForDnMetricValue(NS_METRICS, "UnderReplicatedBlocks", 0L);
  }

  private void waitForDeletion() throws InterruptedException {
    // Wait for more than DATANODE_COUNT replication intervals to ensure all
    // the blocks pending deletion are sent for deletion to the datanodes.
    Thread.sleep(DFS_REPLICATION_INTERVAL * (DATANODE_COUNT + 1) * 1000);
  }

  /**
   * Wait for the named gauge value from the metrics source to reach the
   * desired value.
   *
   * There's an initial delay then a spin cycle of sleep and poll. Because
   * all the tests use a shared FS instance, these tests are not independent;
   * that's why the initial sleep is in there.
   *
   * @param source metrics source
   * @param name gauge name
   * @param expected expected value
   * @return the last metrics record polled
   * @throws Exception if something went wrong.
   */
  private MetricsRecordBuilder waitForDnMetricValue(String source,
                                                    String name,
                                                    long expected)
      throws Exception {
    MetricsRecordBuilder rb;
    long gauge;
    //initial wait.
    waitForDeletion();
    //lots of retries are allowed for slow systems; fast ones will still
    //exit early
    int retries = (DATANODE_COUNT + 1) * WAIT_GAUGE_VALUE_RETRIES;
    rb = getMetrics(source);
    gauge = MetricsAsserts.getLongGauge(name, rb);
    while (gauge != expected && (--retries > 0)) {
      Thread.sleep(DFS_REPLICATION_INTERVAL * 500);
      rb = getMetrics(source);
      gauge = MetricsAsserts.getLongGauge(name, rb);
    }
    //at this point the assertion is valid or the retry count ran out
    assertGauge(name, expected, rb);
    return rb;
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
    
    cluster.getNameNodeRpc().setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
    cluster.getNameNodeRpc().saveNamespace();
    cluster.getNameNodeRpc().setSafeMode(SafeModeAction.SAFEMODE_LEAVE, false);
    updateMetrics();
    
    long newLastCkptTime = MetricsAsserts.getLongGauge("LastCheckpointTime",
        getMetrics(NS_METRICS));
    assertTrue(lastCkptTime < newLastCkptTime);
    assertGauge("LastWrittenTransactionId", 6L, getMetrics(NS_METRICS));
    assertGauge("TransactionsSinceLastCheckpoint", 1L, getMetrics(NS_METRICS));
    assertGauge("TransactionsSinceLastLogRoll", 1L, getMetrics(NS_METRICS));
  }
  
  /**
   * Tests that the sync and block report metrics get updated on cluster
   * startup.
   */
  @Test
  public void testSyncAndBlockReportMetric() throws Exception {
    MetricsRecordBuilder rb = getMetrics(NN_METRICS);
    // We have one sync when the cluster starts up, just opening the journal
    assertCounter("SyncsNumOps", 1L, rb);
    // Each datanode reports in when the cluster comes up
    assertCounter("BlockReportNumOps",
                  (long)DATANODE_COUNT * cluster.getStoragesPerDatanode(), rb);
    
    // Sleep for an interval+slop to let the percentiles rollover
    Thread.sleep((PERCENTILES_INTERVAL+1)*1000);
    
    // Check that the percentiles were updated
    assertQuantileGauges("Syncs1s", rb);
    assertQuantileGauges("BlockReport1s", rb);
  }

  /**
   * Test NN ReadOps Count and WriteOps Count
   */
  @Test
  public void testReadWriteOps() throws Exception {
    MetricsRecordBuilder rb = getMetrics(NN_METRICS);
    long startWriteCounter = MetricsAsserts.getLongCounter("TransactionsNumOps",
        rb);
    Path file1_Path = new Path(TEST_ROOT_DIR_PATH, "ReadData.dat");

    //Perform create file operation
    createFile(file1_Path, 1024 * 1024,(short)2);

    // Perform read file operation on earlier created file
    readFile(fs, file1_Path);
    MetricsRecordBuilder rbNew = getMetrics(NN_METRICS);
    assertTrue(MetricsAsserts.getLongCounter("TransactionsNumOps", rbNew) >
        startWriteCounter);
  }
}
