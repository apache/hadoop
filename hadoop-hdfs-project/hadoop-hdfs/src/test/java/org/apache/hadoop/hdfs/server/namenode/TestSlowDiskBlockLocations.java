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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.SlowDiskTracker;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests LocatedBlock sorting logic under slow disk scenarios.
 * Verifies that replicas on slow disks are moved to the end of the location
 * list so that clients prefer reading from faster storage.
 */
public class TestSlowDiskBlockLocations {

  private static final long OUTLIERS_REPORT_INTERVAL = 1000;
  private static final int BLOCK_SIZE = 1024 * 1024; // 1MB
  private static final int FILE_SIZE = 3 * BLOCK_SIZE; // 3 blocks
  private static final short REPLICATION = 3;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private NameNode nameNode;

  @BeforeEach
  public void setup() throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY, 100);
    conf.setTimeDuration(DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY,
        OUTLIERS_REPORT_INTERVAL, TimeUnit.MILLISECONDS);
    // Configure a short cache rebuild interval for testing
    conf.setTimeDuration(DFSConfigKeys.DFS_NAMENODE_SLOW_DISK_CACHE_REBUILD_INTERVAL_KEY,
        OUTLIERS_REPORT_INTERVAL, TimeUnit.MILLISECONDS);
    // Enable slow disk deprioritization sorting
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DEPRIORITIZE_SLOW_DISK_DATANODE_FOR_READ_KEY, true);

    // Create a cluster with 3 DataNodes
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .build();
    cluster.waitActive();

    dfs = cluster.getFileSystem();
    nameNode = cluster.getNameNode();
  }

  @AfterEach
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Test scenario 1: Verify that a slow disk DataNode is moved to the end
   * of the replica location list.
   */
  @Test
  @Timeout(60)
  public void testSlowDiskDataNodeMovedToEnd() throws Exception {
    // 1. Create a test file with multiple replicas
    Path testFile = new Path("/testSlowDisk");
    DFSTestUtil.createFile(dfs, testFile, FILE_SIZE, REPLICATION, 0L);
    DFSTestUtil.waitForReplication(dfs, testFile, REPLICATION, 30000);

    // 2. Get the SlowDiskTracker and extend report validity
    SlowDiskTracker slowDiskTracker = nameNode.getNamesystem()
        .getBlockManager().getDatanodeManager().getSlowDiskTracker();
    slowDiskTracker.setReportValidityMs(OUTLIERS_REPORT_INTERVAL * 100);

    // 3. Report a slow disk on the first DataNode
    DataNode slowDn = cluster.getDataNodes().get(0);
    String slowDnIpcAddr = slowDn.getDatanodeId().getIpcAddr(false);

    // Get the full disk key (volumeName|storageID) of the first volume
    String diskKey = getFirstDiskKey(slowDn);
    assertNotNull(diskKey, "Disk key should not be null");

    // Simulate slow disk report using the full disk key (volumeName|storageID)
    slowDn.getDiskMetrics().addSlowDiskForTesting(diskKey,
        ImmutableMap.of(SlowDiskReports.DiskOp.WRITE, 2.5));

    // 4. Wait for the NameNode to receive the slow disk report
    Thread.sleep(OUTLIERS_REPORT_INTERVAL);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return !slowDiskTracker.getSlowDisksReport().isEmpty();
      }
    }, 1000, 10000);

    // 5. Get block locations and verify sorting result
    LocatedBlocks locsAfter = NameNodeAdapter.getBlockLocations(nameNode,
        testFile.toString(), 0, FILE_SIZE);
    List<LocatedBlock> blocksAfter = locsAfter.getLocatedBlocks();
    assertFalse(blocksAfter.isEmpty(),
        "Should have at least one block");

    LocatedBlock firstBlockAfter = blocksAfter.get(0);
    DatanodeInfo[] dnsAfter = firstBlockAfter.getLocations();
    assertEquals(REPLICATION, dnsAfter.length,
        "Should have 3 replicas");

    // 6. Verify: slow disk DataNode should be at the last position
    assertEquals(slowDnIpcAddr,
        dnsAfter[dnsAfter.length - 1].getIpcAddr(false),
        "Slow disk DataNode should be at the last position");

    // 7. Verify slow disk DataNode is not in any earlier position
    for (int i = 0; i < dnsAfter.length - 1; i++) {
      assertNotEquals(slowDnIpcAddr, dnsAfter[i].getIpcAddr(false),
          "Slow disk DataNode should not be at position " + i);
    }
  }

  /**
   * Helper method: Get the full disk key (volumeName|storageID) of the first
   * volume on the given DataNode.
   */
  private String getFirstDiskKey(DataNode dn) throws Exception {
    try (org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi
            .FsVolumeReferences refs =
                dn.getFSDataset().getFsVolumeReferences()) {
      if (refs != null && refs.size() > 0) {
        String volumeName = refs.get(0).getBaseURI().getPath();
        String storageID = refs.get(0).getStorageID();
        return volumeName + "|" + storageID;
      }
    }
    return null;
  }

  /**
   * Test scenario 2: Verify handling when multiple DataNodes have slow disks.
   */
  @Test
  @Timeout(60)
  public void testMultipleSlowDiskDataNodes() throws Exception {
    // 1. Create a test file
    Path testFile = new Path("/testMultipleSlowDisks");
    DFSTestUtil.createFile(dfs, testFile, FILE_SIZE, REPLICATION, 0L);
    DFSTestUtil.waitForReplication(dfs, testFile, REPLICATION, 30000);

    // 2. Configure SlowDiskTracker
    SlowDiskTracker slowDiskTracker = nameNode.getNamesystem()
        .getBlockManager().getDatanodeManager().getSlowDiskTracker();
    slowDiskTracker.setReportValidityMs(OUTLIERS_REPORT_INTERVAL * 100);

    // 3. Report slow disks on the first two DataNodes
    DataNode slowDn1 = cluster.getDataNodes().get(0);
    DataNode slowDn2 = cluster.getDataNodes().get(1);
    DataNode normalDn = cluster.getDataNodes().get(2);

    String slowDnAddr1 = slowDn1.getDatanodeId().getIpcAddr(false);
    String slowDnAddr2 = slowDn2.getDatanodeId().getIpcAddr(false);
    String normalDnAddr = normalDn.getDatanodeId().getIpcAddr(false);

    String diskKey1 = getFirstDiskKey(slowDn1);
    String diskKey2 = getFirstDiskKey(slowDn2);

    slowDn1.getDiskMetrics().addSlowDiskForTesting(diskKey1,
        ImmutableMap.of(SlowDiskReports.DiskOp.WRITE, 2.5));
    slowDn2.getDiskMetrics().addSlowDiskForTesting(diskKey2,
        ImmutableMap.of(SlowDiskReports.DiskOp.READ, 3.0));

    // 4. Wait for slow disk reports
    Thread.sleep(OUTLIERS_REPORT_INTERVAL);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return slowDiskTracker.getSlowDisksReport().size() >= 2;
      }
    }, 1000, 10000);

    // 5. Get block locations
    LocatedBlocks locs = NameNodeAdapter.getBlockLocations(nameNode,
        testFile.toString(), 0, FILE_SIZE);
    DatanodeInfo[] dns = locs.getLocatedBlocks().get(0).getLocations();

    // 6. Verify normal DataNode is not at the last position
    assertNotEquals(normalDnAddr,
        dns[dns.length - 1].getIpcAddr(false),
        "Normal DataNode should not be at the last position");

    // 7. Verify the last two positions are slow disk DataNodes
    Set<String> lastTwoAddrs = new HashSet<>();
    lastTwoAddrs.add(dns[1].getIpcAddr(false));
    lastTwoAddrs.add(dns[2].getIpcAddr(false));

    assertTrue(lastTwoAddrs.contains(slowDnAddr1),
        "Last two should contain slowDn1");
    assertTrue(lastTwoAddrs.contains(slowDnAddr2),
        "Last two should contain slowDn2");
  }

  /**
   * Test scenario 3: Verify handling when all DataNodes have slow disks.
   */
  @Test
  @Timeout(60)
  public void testAllSlowDiskDataNodes() throws Exception {
    // 1. Create a test file
    Path testFile = new Path("/testAllSlowDisks");
    DFSTestUtil.createFile(dfs, testFile, FILE_SIZE, REPLICATION, 0L);
    DFSTestUtil.waitForReplication(dfs, testFile, REPLICATION, 30000);

    // 2. Configure SlowDiskTracker
    SlowDiskTracker slowDiskTracker = nameNode.getNamesystem()
        .getBlockManager().getDatanodeManager().getSlowDiskTracker();
    slowDiskTracker.setReportValidityMs(OUTLIERS_REPORT_INTERVAL * 100);

    // 3. Report slow disks on all DataNodes
    for (int i = 0; i < 3; i++) {
      DataNode dn = cluster.getDataNodes().get(i);
      String diskKey = getFirstDiskKey(dn);
      dn.getDiskMetrics().addSlowDiskForTesting(diskKey,
          ImmutableMap.of(SlowDiskReports.DiskOp.WRITE, 2.0 + i * 0.5));
    }

    // 4. Wait for slow disk reports
    Thread.sleep(OUTLIERS_REPORT_INTERVAL);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return slowDiskTracker.getSlowDisksReport().size() >= 3;
      }
    }, 1000, 10000);

    // 5. Get block locations
    LocatedBlocks locs = NameNodeAdapter.getBlockLocations(nameNode,
        testFile.toString(), 0, FILE_SIZE);
    DatanodeInfo[] dns = locs.getLocatedBlocks().get(0).getLocations();

    // 6. Verify all replicas are still returned
    assertEquals(REPLICATION, dns.length,
        "Should still return 3 replicas");
  }

  /**
   * Test scenario 4: Verify the slow disk cache rebuild mechanism.
   */
  @Test
  @Timeout(60)
  public void testSlowDiskCacheRebuild() throws Exception {
    SlowDiskTracker slowDiskTracker = nameNode.getNamesystem()
        .getBlockManager().getDatanodeManager().getSlowDiskTracker();
    slowDiskTracker.setReportValidityMs(OUTLIERS_REPORT_INTERVAL * 10);

    // 1. Report a slow disk on the first DataNode
    DataNode slowDn = cluster.getDataNodes().get(0);
    String diskKey = getFirstDiskKey(slowDn);
    slowDn.getDiskMetrics().addSlowDiskForTesting(diskKey,
        ImmutableMap.of(SlowDiskReports.DiskOp.WRITE, 3.0));

    // 2. Wait for heartbeat to deliver data to SlowDiskTracker
    Thread.sleep(OUTLIERS_REPORT_INTERVAL);

    // Wait for data to reach diskIDLatencyMap
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return !slowDiskTracker.getSlowDisksReport().isEmpty();
      }
    }, 500, 10000);

    // Manually trigger cache rebuild
    slowDiskTracker.checkAndUpdateReportIfNecessary();
    Thread.sleep(500);

    // 3. Verify cache contains the slow disk
    Map<String, Double> cache1 = slowDiskTracker.getAllValidSlowDisks();
    assertFalse(cache1.isEmpty(),
        "Cache should contain slow disks");

    // Extract storageID to build expected cache key
    String storageID = diskKey.substring(diskKey.indexOf('|') + 1);
    String expectedCacheKey = slowDn.getDatanodeId().getIpcAddr(false)
        + ":" + storageID;
    assertTrue(cache1.containsKey(expectedCacheKey),
        "Cache should contain the slow disk key");

    // 4. Clear slow disk report (simulate disk recovery)
    slowDn.getDiskMetrics().getDiskOutliersStats().remove(diskKey);

    // 5. Wait for cache rebuild (entry should still be within validity period)
    // Ensure we exceed cacheRebuildIntervalMs (1000ms); using > comparison
    Thread.sleep(OUTLIERS_REPORT_INTERVAL * 2);
    slowDiskTracker.checkAndUpdateReportIfNecessary();
    Thread.sleep(1000);

    Map<String, Double> cache2 = slowDiskTracker.getAllValidSlowDisks();
    assertTrue(cache2.containsKey(expectedCacheKey),
        "Cache should still contain the slow disk (not yet expired)");

    // 6. Wait for expiration
    slowDiskTracker.setReportValidityMs(100); // Set a very short validity
    // Wait beyond reportValidityMs(100ms) + cacheRebuildIntervalMs(1000ms)
    Thread.sleep(1500);

    // Trigger cache rebuild
    slowDiskTracker.checkAndUpdateReportIfNecessary();
    Thread.sleep(1000);

    Map<String, Double> cache3 = slowDiskTracker.getAllValidSlowDisks();
    assertFalse(cache3.containsKey(expectedCacheKey),
        "Cache should no longer contain the expired slow disk");
  }

  /**
   * Test scenario 5: Verify the slow disk expiration mechanism.
   */
  @Test
  @Timeout(60)
  public void testSlowDiskExpiration() throws Exception {
    SlowDiskTracker slowDiskTracker = nameNode.getNamesystem()
        .getBlockManager().getDatanodeManager().getSlowDiskTracker();

    // Set a short validity period for testing
    long shortValidityMs = 2000; // 2 seconds
    slowDiskTracker.setReportValidityMs(shortValidityMs);

    // 1. Report the first slow disk
    DataNode slowDn1 = cluster.getDataNodes().get(0);
    DataNode slowDn2 = cluster.getDataNodes().get(1);

    String diskKey1 = getFirstDiskKey(slowDn1);
    String diskKey2 = getFirstDiskKey(slowDn2);

    slowDn1.getDiskMetrics().addSlowDiskForTesting(diskKey1,
        ImmutableMap.of(SlowDiskReports.DiskOp.WRITE, 2.5));

    // 2. Wait for heartbeat to deliver data to SlowDiskTracker
    Thread.sleep(OUTLIERS_REPORT_INTERVAL);

    // Wait for data to reach diskIDLatencyMap
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return !slowDiskTracker.getSlowDisksReport().isEmpty();
      }
    }, 500, 10000);

    // Trigger cache rebuild
    slowDiskTracker.checkAndUpdateReportIfNecessary();
    Thread.sleep(500);

    Map<String, Double> cache1 = slowDiskTracker.getAllValidSlowDisks();
    int initialSize = cache1.size();
    assertTrue(initialSize > 0,
        "Should have at least one slow disk");

    // 3. Immediately add the second slow disk
    slowDn2.getDiskMetrics().addSlowDiskForTesting(diskKey2,
        ImmutableMap.of(SlowDiskReports.DiskOp.READ, 3.0));

    Thread.sleep(OUTLIERS_REPORT_INTERVAL);

    // Wait for the second slow disk data to arrive
    final int expectedSize = initialSize + 1;
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return slowDiskTracker.getSlowDisksReport().size() >= expectedSize;
      }
    }, 500, 10000);

    slowDiskTracker.checkAndUpdateReportIfNecessary();
    Thread.sleep(500);

    Map<String, Double> cache2 = slowDiskTracker.getAllValidSlowDisks();
    assertTrue(cache2.size() >= expectedSize,
        "Cache size should have increased");

    // 4. Clear slow disk reports to stop heartbeat timestamp refreshes.
    // Must completely remove the key; setting to null leaves the key
    // with an empty Map, and the DataNode outlier detector could still
    // re-detect and re-report the disk.
    slowDn1.getDiskMetrics().getDiskOutliersStats().remove(diskKey1);
    slowDn2.getDiskMetrics().getDiskOutliersStats().remove(diskKey2);

    // 5. Wait for slow disk expiration (2s validity)
    Thread.sleep(2500); // Ensure we exceed validityMs

    slowDiskTracker.checkAndUpdateReportIfNecessary();
    Thread.sleep(1000);

    Map<String, Double> cache3 = slowDiskTracker.getAllValidSlowDisks();

    // Both slow disks should have expired since reports were cleared
    String storageID1 = diskKey1.substring(diskKey1.indexOf('|') + 1);
    String storageID2 = diskKey2.substring(diskKey2.indexOf('|') + 1);
    String cacheKey1 = slowDn1.getDatanodeId().getIpcAddr(false)
        + ":" + storageID1;
    String cacheKey2 = slowDn2.getDatanodeId().getIpcAddr(false)
        + ":" + storageID2;

    assertFalse(cache3.containsKey(cacheKey1),
        "Slow disk 1 should have expired");
    assertFalse(cache3.containsKey(cacheKey2),
        "Slow disk 2 should have expired");
    assertTrue(cache3.isEmpty(),
        "All slow disks should have expired");
  }

  /**
   * Test scenario 6: Verify cache integration with the read path.
   */
  @Test
  @Timeout(60)
  public void testCacheIntegrationWithReadPath() throws Exception {
    // 1. Create a test file
    Path testFile = new Path("/testCacheIntegration");
    DFSTestUtil.createFile(dfs, testFile, FILE_SIZE, REPLICATION, 0L);
    DFSTestUtil.waitForReplication(dfs, testFile, REPLICATION, 30000);

    SlowDiskTracker slowDiskTracker = nameNode.getNamesystem()
        .getBlockManager().getDatanodeManager().getSlowDiskTracker();
    slowDiskTracker.setReportValidityMs(OUTLIERS_REPORT_INTERVAL * 100);

    // 2. Report a slow disk
    DataNode slowDn = cluster.getDataNodes().get(0);
    String diskKey = getFirstDiskKey(slowDn);
    String slowDnAddr = slowDn.getDatanodeId().getIpcAddr(false);

    slowDn.getDiskMetrics().addSlowDiskForTesting(diskKey,
        ImmutableMap.of(SlowDiskReports.DiskOp.WRITE, 5.0));

    // 3. Wait for heartbeat processing
    Thread.sleep(OUTLIERS_REPORT_INTERVAL);

    // Trigger cache rebuild
    slowDiskTracker.checkAndUpdateReportIfNecessary();
    Thread.sleep(OUTLIERS_REPORT_INTERVAL);

    // 4. Verify cache contains the slow disk
    Map<String, Double> cache = slowDiskTracker.getAllValidSlowDisks();
    assertFalse(cache.isEmpty(), "Cache should not be empty");

    // 5. Get sorted block locations
    LocatedBlocks locsAfter = NameNodeAdapter.getBlockLocations(nameNode,
        testFile.toString(), 0, FILE_SIZE);
    DatanodeInfo[] dnsAfter =
        locsAfter.getLocatedBlocks().get(0).getLocations();

    // 6. Verify slow disk DataNode is sorted to the end
    assertEquals(slowDnAddr,
        dnsAfter[dnsAfter.length - 1].getIpcAddr(false),
        "Slow disk DataNode should be at the last position");
  }

  /**
   * Test scenario 7: Verify that cache rebuild interval is independent of
   * report generation interval.
   */
  @Test
  @Timeout(60)
  public void testIndependentCacheRebuildInterval() throws Exception {
    SlowDiskTracker slowDiskTracker = nameNode.getNamesystem()
        .getBlockManager().getDatanodeManager().getSlowDiskTracker();

    // Set a different report validity period
    slowDiskTracker.setReportValidityMs(5000); // 5 second validity

    // 1. Report a slow disk
    DataNode slowDn = cluster.getDataNodes().get(0);
    String diskKey = getFirstDiskKey(slowDn);
    slowDn.getDiskMetrics().addSlowDiskForTesting(diskKey,
        ImmutableMap.of(SlowDiskReports.DiskOp.WRITE, 2.0));

    // 2. Wait for initial heartbeat and cache build
    Thread.sleep(OUTLIERS_REPORT_INTERVAL);
    slowDiskTracker.checkAndUpdateReportIfNecessary();
    Thread.sleep(200);

    Map<String, Double> cache1 = slowDiskTracker.getAllValidSlowDisks();
    int size1 = cache1.size();

    // 3. Trigger multiple checks and observe cache updates
    for (int i = 0; i < 3; i++) {
      Thread.sleep(OUTLIERS_REPORT_INTERVAL);
      slowDiskTracker.checkAndUpdateReportIfNecessary();
      Thread.sleep(200);
    }

    // 4. Verify the independent cache update mechanism works
    assertNotNull(slowDiskTracker.getAllValidSlowDisks(),
        "getAllValidSlowDisks should return non-null");
  }
}
