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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi.FsVolumeReferences;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.FakeTimer;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DN_CACHED_DFSUSED_CHECK_INTERVAL_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFsDatasetImpl {
  Logger LOG = LoggerFactory.getLogger(TestFsDatasetImpl.class);
  private static final String BASE_DIR =
      new FileSystemTestHelper().getTestRootDir();
  private static final int NUM_INIT_VOLUMES = 2;
  private static final String CLUSTER_ID = "cluser-id";
  private static final String[] BLOCK_POOL_IDS = {"bpid-0", "bpid-1"};

  // Use to generate storageUuid
  private static final DataStorage dsForStorageUuid = new DataStorage(
      new StorageInfo(HdfsServerConstants.NodeType.DATA_NODE));

  private Configuration conf;
  private DataNode datanode;
  private DataStorage storage;
  private FsDatasetImpl dataset;
  
  private final static String BLOCKPOOL = "BP-TEST";

  private static Storage.StorageDirectory createStorageDirectory(File root,
      Configuration conf)
      throws SecurityException, IOException {
    Storage.StorageDirectory sd = new Storage.StorageDirectory(
        StorageLocation.parse(root.toURI().toString()));
    DataStorage.createStorageID(sd, false, conf);
    return sd;
  }

  private static void createStorageDirs(DataStorage storage, Configuration conf,
      int numDirs) throws IOException {
    List<Storage.StorageDirectory> dirs =
        new ArrayList<Storage.StorageDirectory>();
    List<String> dirStrings = new ArrayList<String>();
    FileUtils.deleteDirectory(new File(BASE_DIR));
    for (int i = 0; i < numDirs; i++) {
      File loc = new File(BASE_DIR + "/data" + i);
      dirStrings.add(new Path(loc.toString()).toUri().toString());
      loc.mkdirs();
      dirs.add(createStorageDirectory(loc, conf));
      when(storage.getStorageDir(i)).thenReturn(dirs.get(i));
    }

    String dataDir = StringUtils.join(",", dirStrings);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDir);
    when(storage.dirIterator()).thenReturn(dirs.iterator());
    when(storage.getNumStorageDirs()).thenReturn(numDirs);
  }

  private int getNumVolumes() {
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      return volumes.size();
    } catch (IOException e) {
      return 0;
    }
  }

  @Before
  public void setUp() throws IOException {
    datanode = mock(DataNode.class);
    storage = mock(DataStorage.class);
    this.conf = new Configuration();
    this.conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 0);

    when(datanode.getConf()).thenReturn(conf);
    final DNConf dnConf = new DNConf(datanode);
    when(datanode.getDnConf()).thenReturn(dnConf);
    final BlockScanner disabledBlockScanner = new BlockScanner(datanode);
    when(datanode.getBlockScanner()).thenReturn(disabledBlockScanner);
    final ShortCircuitRegistry shortCircuitRegistry =
        new ShortCircuitRegistry(conf);
    when(datanode.getShortCircuitRegistry()).thenReturn(shortCircuitRegistry);

    createStorageDirs(storage, conf, NUM_INIT_VOLUMES);
    dataset = new FsDatasetImpl(datanode, storage, conf);
    for (String bpid : BLOCK_POOL_IDS) {
      dataset.addBlockPool(bpid, conf);
    }

    assertEquals(NUM_INIT_VOLUMES, getNumVolumes());
    assertEquals(0, dataset.getNumFailedVolumes());
  }

  @Test
  public void testAddVolumes() throws IOException {
    final int numNewVolumes = 3;
    final int numExistingVolumes = getNumVolumes();
    final int totalVolumes = numNewVolumes + numExistingVolumes;
    Set<String> expectedVolumes = new HashSet<String>();
    List<NamespaceInfo> nsInfos = Lists.newArrayList();
    for (String bpid : BLOCK_POOL_IDS) {
      nsInfos.add(new NamespaceInfo(0, CLUSTER_ID, bpid, 1));
    }
    for (int i = 0; i < numNewVolumes; i++) {
      String path = BASE_DIR + "/newData" + i;
      String pathUri = new Path(path).toUri().toString();
      expectedVolumes.add(new File(pathUri).getAbsolutePath());
      StorageLocation loc = StorageLocation.parse(pathUri);
      Storage.StorageDirectory sd = createStorageDirectory(
          new File(path), conf);
      DataStorage.VolumeBuilder builder =
          new DataStorage.VolumeBuilder(storage, sd);
      when(storage.prepareVolume(eq(datanode), eq(loc),
          anyListOf(NamespaceInfo.class)))
          .thenReturn(builder);

      dataset.addVolume(loc, nsInfos);
      LOG.info("expectedVolumes " + i + " is " +
          new File(pathUri).getAbsolutePath());
    }

    assertEquals(totalVolumes, getNumVolumes());
    assertEquals(totalVolumes, dataset.storageMap.size());

    Set<String> actualVolumes = new HashSet<String>();
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      for (int i = 0; i < numNewVolumes; i++) {
        String volumeName = volumes.get(numExistingVolumes + i).toString();
        actualVolumes.add(volumeName);
        LOG.info("actualVolume " + i + " is " + volumeName);
      }
    }
    assertEquals(actualVolumes.size(), expectedVolumes.size());
    assertTrue(actualVolumes.containsAll(expectedVolumes));
  }

  @Test
  public void testAddVolumeWithSameStorageUuid() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    try {
      cluster.waitActive();
      assertTrue(cluster.getDataNodes().get(0).isConnectedToNN(
          cluster.getNameNode().getServiceRpcAddress()));

      MiniDFSCluster.DataNodeProperties dn = cluster.stopDataNode(0);
      File vol0 = cluster.getStorageDir(0, 0);
      File vol1 = cluster.getStorageDir(0, 1);
      Storage.StorageDirectory sd0 = new Storage.StorageDirectory(vol0);
      Storage.StorageDirectory sd1 = new Storage.StorageDirectory(vol1);
      FileUtils.copyFile(sd0.getVersionFile(), sd1.getVersionFile());

      cluster.restartDataNode(dn, true);
      cluster.waitActive();
      assertFalse(cluster.getDataNodes().get(0).isConnectedToNN(
          cluster.getNameNode().getServiceRpcAddress()));
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testRemoveVolumes() throws IOException {
    // Feed FsDataset with block metadata.
    final int NUM_BLOCKS = 100;
    for (int i = 0; i < NUM_BLOCKS; i++) {
      String bpid = BLOCK_POOL_IDS[NUM_BLOCKS % BLOCK_POOL_IDS.length];
      ExtendedBlock eb = new ExtendedBlock(bpid, i);
      try (ReplicaHandler replica =
          dataset.createRbw(StorageType.DEFAULT, null, eb, false)) {
      }
    }
    final String[] dataDirs =
        conf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY).split(",");
    final String volumePathToRemove = dataDirs[0];
    Set<StorageLocation> volumesToRemove = new HashSet<>();
    volumesToRemove.add(StorageLocation.parse(volumePathToRemove));

    FsVolumeReferences volReferences = dataset.getFsVolumeReferences();
    FsVolumeImpl volumeToRemove = null;
    for (FsVolumeSpi vol: volReferences) {
      if (vol.getStorageLocation().equals(volumesToRemove.iterator().next())) {
        volumeToRemove = (FsVolumeImpl) vol;
      }
    }
    assertTrue(volumeToRemove != null);
    volReferences.close();
    dataset.removeVolumes(volumesToRemove, true);
    int expectedNumVolumes = dataDirs.length - 1;
    assertEquals("The volume has been removed from the volumeList.",
        expectedNumVolumes, getNumVolumes());
    assertEquals("The volume has been removed from the storageMap.",
        expectedNumVolumes, dataset.storageMap.size());

    try {
      dataset.asyncDiskService.execute(volumeToRemove,
          new Runnable() {
            @Override
            public void run() {}
          });
      fail("Expect RuntimeException: the volume has been removed from the "
           + "AsyncDiskService.");
    } catch (RuntimeException e) {
      GenericTestUtils.assertExceptionContains("Cannot find volume", e);
    }

    int totalNumReplicas = 0;
    for (String bpid : dataset.volumeMap.getBlockPoolList()) {
      totalNumReplicas += dataset.volumeMap.size(bpid);
    }
    assertEquals("The replica infos on this volume has been removed from the "
                 + "volumeMap.", NUM_BLOCKS / NUM_INIT_VOLUMES,
                 totalNumReplicas);
  }

  @Test(timeout = 5000)
  public void testRemoveNewlyAddedVolume() throws IOException {
    final int numExistingVolumes = getNumVolumes();
    List<NamespaceInfo> nsInfos = new ArrayList<>();
    for (String bpid : BLOCK_POOL_IDS) {
      nsInfos.add(new NamespaceInfo(0, CLUSTER_ID, bpid, 1));
    }
    String newVolumePath = BASE_DIR + "/newVolumeToRemoveLater";
    StorageLocation loc = StorageLocation.parse(newVolumePath);

    Storage.StorageDirectory sd = createStorageDirectory(
        new File(newVolumePath), conf);
    DataStorage.VolumeBuilder builder =
        new DataStorage.VolumeBuilder(storage, sd);
    when(storage.prepareVolume(eq(datanode), eq(loc),
        anyListOf(NamespaceInfo.class)))
        .thenReturn(builder);

    dataset.addVolume(loc, nsInfos);
    assertEquals(numExistingVolumes + 1, getNumVolumes());

    when(storage.getNumStorageDirs()).thenReturn(numExistingVolumes + 1);
    when(storage.getStorageDir(numExistingVolumes)).thenReturn(sd);
    Set<StorageLocation> volumesToRemove = new HashSet<>();
    volumesToRemove.add(loc);
    dataset.removeVolumes(volumesToRemove, true);
    assertEquals(numExistingVolumes, getNumVolumes());
  }

  @Test
  public void testAddVolumeFailureReleasesInUseLock() throws IOException {
    FsDatasetImpl spyDataset = spy(dataset);
    FsVolumeImpl mockVolume = mock(FsVolumeImpl.class);
    File badDir = new File(BASE_DIR, "bad");
    badDir.mkdirs();
    doReturn(mockVolume).when(spyDataset)
        .createFsVolume(anyString(), any(StorageDirectory.class),
            any(StorageLocation.class));
    doThrow(new IOException("Failed to getVolumeMap()"))
      .when(mockVolume).getVolumeMap(
        anyString(),
        any(ReplicaMap.class),
        any(RamDiskReplicaLruTracker.class));

    Storage.StorageDirectory sd = createStorageDirectory(badDir, conf);
    sd.lock();
    DataStorage.VolumeBuilder builder = new DataStorage.VolumeBuilder(storage, sd);
    when(storage.prepareVolume(eq(datanode),
        eq(StorageLocation.parse(badDir.toURI().toString())),
        Matchers.<List<NamespaceInfo>>any()))
        .thenReturn(builder);

    StorageLocation location = StorageLocation.parse(badDir.toString());
    List<NamespaceInfo> nsInfos = Lists.newArrayList();
    for (String bpid : BLOCK_POOL_IDS) {
      nsInfos.add(new NamespaceInfo(0, CLUSTER_ID, bpid, 1));
    }

    try {
      spyDataset.addVolume(location, nsInfos);
      fail("Expect to throw MultipleIOException");
    } catch (MultipleIOException e) {
    }

    FsDatasetTestUtil.assertFileLockReleased(badDir.toString());
  }
  
  @Test
  public void testDeletingBlocks() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      DataNode dn = cluster.getDataNodes().get(0);
      
      FsDatasetSpi<?> ds = DataNodeTestUtils.getFSDataset(dn);
      ds.addBlockPool(BLOCKPOOL, conf);
      FsVolumeImpl vol;
      try (FsDatasetSpi.FsVolumeReferences volumes = ds.getFsVolumeReferences()) {
        vol = (FsVolumeImpl)volumes.get(0);
      }

      ExtendedBlock eb;
      ReplicaInfo info;
      List<Block> blockList = new ArrayList<>();
      for (int i = 1; i <= 63; i++) {
        eb = new ExtendedBlock(BLOCKPOOL, i, 1, 1000 + i);
        cluster.getFsDatasetTestUtils(0).createFinalizedReplica(eb);
        blockList.add(eb.getLocalBlock());
      }
      ds.invalidate(BLOCKPOOL, blockList.toArray(new Block[0]));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // Nothing to do
      }
      assertTrue(ds.isDeletingBlock(BLOCKPOOL, blockList.get(0).getBlockId()));

      blockList.clear();
      eb = new ExtendedBlock(BLOCKPOOL, 64, 1, 1064);
      cluster.getFsDatasetTestUtils(0).createFinalizedReplica(eb);
      blockList.add(eb.getLocalBlock());
      ds.invalidate(BLOCKPOOL, blockList.toArray(new Block[0]));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // Nothing to do
      }
      assertFalse(ds.isDeletingBlock(BLOCKPOOL, blockList.get(0).getBlockId()));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testDuplicateReplicaResolution() throws IOException {
    FsVolumeImpl fsv1 = Mockito.mock(FsVolumeImpl.class);
    FsVolumeImpl fsv2 = Mockito.mock(FsVolumeImpl.class);

    File f1 = new File("d1/block");
    File f2 = new File("d2/block");

    ReplicaInfo replicaOlder = new FinalizedReplica(1,1,1,fsv1,f1);
    ReplicaInfo replica = new FinalizedReplica(1,2,2,fsv1,f1);
    ReplicaInfo replicaSame = new FinalizedReplica(1,2,2,fsv1,f1);
    ReplicaInfo replicaNewer = new FinalizedReplica(1,3,3,fsv1,f1);

    ReplicaInfo replicaOtherOlder = new FinalizedReplica(1,1,1,fsv2,f2);
    ReplicaInfo replicaOtherSame = new FinalizedReplica(1,2,2,fsv2,f2);
    ReplicaInfo replicaOtherNewer = new FinalizedReplica(1,3,3,fsv2,f2);

    // equivalent path so don't remove either
    assertNull(BlockPoolSlice.selectReplicaToDelete(replicaSame, replica));
    assertNull(BlockPoolSlice.selectReplicaToDelete(replicaOlder, replica));
    assertNull(BlockPoolSlice.selectReplicaToDelete(replicaNewer, replica));

    // keep latest found replica
    assertSame(replica,
        BlockPoolSlice.selectReplicaToDelete(replicaOtherSame, replica));
    assertSame(replicaOtherOlder,
        BlockPoolSlice.selectReplicaToDelete(replicaOtherOlder, replica));
    assertSame(replica,
        BlockPoolSlice.selectReplicaToDelete(replicaOtherNewer, replica));
  }

  @Test
  public void testLoadingDfsUsedForVolumes() throws IOException,
      InterruptedException {
    long waitIntervalTime = 5000;
    // Initialize the cachedDfsUsedIntervalTime larger than waitIntervalTime
    // to avoid cache-dfsused time expired
    long cachedDfsUsedIntervalTime = waitIntervalTime + 1000;
    conf.setLong(DFS_DN_CACHED_DFSUSED_CHECK_INTERVAL_MS,
        cachedDfsUsedIntervalTime);

    long cacheDfsUsed = 1024;
    long dfsUsed = getDfsUsedValueOfNewVolume(cacheDfsUsed, waitIntervalTime);

    assertEquals(cacheDfsUsed, dfsUsed);
  }

  @Test
  public void testLoadingDfsUsedForVolumesExpired() throws IOException,
      InterruptedException {
    long waitIntervalTime = 5000;
    // Initialize the cachedDfsUsedIntervalTime smaller than waitIntervalTime
    // to make cache-dfsused time expired
    long cachedDfsUsedIntervalTime = waitIntervalTime - 1000;
    conf.setLong(DFS_DN_CACHED_DFSUSED_CHECK_INTERVAL_MS,
        cachedDfsUsedIntervalTime);

    long cacheDfsUsed = 1024;
    long dfsUsed = getDfsUsedValueOfNewVolume(cacheDfsUsed, waitIntervalTime);

    // Because the cache-dfsused expired and the dfsUsed will be recalculated
    assertTrue(cacheDfsUsed != dfsUsed);
  }

  private long getDfsUsedValueOfNewVolume(long cacheDfsUsed,
      long waitIntervalTime) throws IOException, InterruptedException {
    List<NamespaceInfo> nsInfos = Lists.newArrayList();
    nsInfos.add(new NamespaceInfo(0, CLUSTER_ID, BLOCK_POOL_IDS[0], 1));

    String CURRENT_DIR = "current";
    String DU_CACHE_FILE = BlockPoolSlice.DU_CACHE_FILE;
    String path = BASE_DIR + "/newData0";
    String pathUri = new Path(path).toUri().toString();
    StorageLocation loc = StorageLocation.parse(pathUri);
    Storage.StorageDirectory sd = createStorageDirectory(new File(path), conf);
    DataStorage.VolumeBuilder builder =
        new DataStorage.VolumeBuilder(storage, sd);
    when(
        storage.prepareVolume(eq(datanode), eq(loc),
            anyListOf(NamespaceInfo.class))).thenReturn(builder);

    String cacheFilePath =
        String.format("%s/%s/%s/%s/%s", path, CURRENT_DIR, BLOCK_POOL_IDS[0],
            CURRENT_DIR, DU_CACHE_FILE);
    File outFile = new File(cacheFilePath);

    if (!outFile.getParentFile().exists()) {
      outFile.getParentFile().mkdirs();
    }

    if (outFile.exists()) {
      outFile.delete();
    }

    FakeTimer timer = new FakeTimer();
    try {
      try (Writer out =
          new OutputStreamWriter(new FileOutputStream(outFile),
              StandardCharsets.UTF_8)) {
        // Write the dfsUsed value and the time to cache file
        out.write(Long.toString(cacheDfsUsed) + " "
            + Long.toString(timer.now()));
        out.flush();
      }
    } catch (IOException ioe) {
    }

    dataset.setTimer(timer);
    timer.advance(waitIntervalTime);
    dataset.addVolume(loc, nsInfos);

    // Get the last volume which was just added before
    FsVolumeImpl newVolume;
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      newVolume = (FsVolumeImpl) volumes.get(volumes.size() - 1);
    }
    long dfsUsed = newVolume.getDfsUsed();

    return dfsUsed;
  }

  @Test(timeout = 60000)
  public void testRemoveVolumeBeingWritten() throws Exception {
    // Will write and remove on dn0.
    final ExtendedBlock eb = new ExtendedBlock(BLOCK_POOL_IDS[0], 0);
    final CountDownLatch startFinalizeLatch = new CountDownLatch(1);
    final CountDownLatch blockReportReceivedLatch = new CountDownLatch(1);
    final CountDownLatch volRemoveStartedLatch = new CountDownLatch(1);
    final CountDownLatch volRemoveCompletedLatch = new CountDownLatch(1);
    class BlockReportThread extends Thread {
      public void run() {
        // Lets wait for the volume remove process to start
        try {
          volRemoveStartedLatch.await();
        } catch (Exception e) {
          LOG.info("Unexpected exception when waiting for vol removal:", e);
        }
        LOG.info("Getting block report");
        dataset.getBlockReports(eb.getBlockPoolId());
        LOG.info("Successfully received block report");
        blockReportReceivedLatch.countDown();
      }
    }

    class ResponderThread extends Thread {
      public void run() {
        try (ReplicaHandler replica = dataset
            .createRbw(StorageType.DEFAULT, null, eb, false)) {
          LOG.info("CreateRbw finished");
          startFinalizeLatch.countDown();

          // Slow down while we're holding the reference to the volume.
          // As we finalize a block, the volume is removed in parallel.
          // Ignore any interrupts coming out of volume shutdown.
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            LOG.info("Ignoring ", ie);
          }

          // Lets wait for the other thread finish getting block report
          blockReportReceivedLatch.await();

          dataset.finalizeBlock(eb, false);
          LOG.info("FinalizeBlock finished");
        } catch (Exception e) {
          LOG.warn("Exception caught. This should not affect the test", e);
        }
      }
    }

    class VolRemoveThread extends Thread {
      public void run() {
        Set<StorageLocation> volumesToRemove = new HashSet<>();
        try {
          volumesToRemove.add(dataset.getVolume(eb).getStorageLocation());
        } catch (Exception e) {
          LOG.info("Problem preparing volumes to remove: ", e);
          Assert.fail("Exception in remove volume thread, check log for " +
              "details.");
        }
        LOG.info("Removing volume " + volumesToRemove);
        dataset.removeVolumes(volumesToRemove, true);
        volRemoveCompletedLatch.countDown();
        LOG.info("Removed volume " + volumesToRemove);
      }
    }

    // Start the volume write operation
    ResponderThread responderThread = new ResponderThread();
    responderThread.start();
    startFinalizeLatch.await();

    // Start the block report get operation
    final BlockReportThread blockReportThread = new BlockReportThread();
    blockReportThread.start();

    // Start the volume remove operation
    VolRemoveThread volRemoveThread = new VolRemoveThread();
    volRemoveThread.start();

    // Let volume write and remove operation be
    // blocked for few seconds
    Thread.sleep(2000);

    // Signal block report receiver and volume writer
    // thread to complete their operations so that vol
    // remove can proceed
    volRemoveStartedLatch.countDown();

    // Verify if block report can be received
    // when volume is in use and also being removed
    blockReportReceivedLatch.await();

    // Verify if volume can be removed safely when there
    // are read/write operation in-progress
    volRemoveCompletedLatch.await();
  }

  /**
   * Tests stopping all the active DataXceiver thread on volume failure event.
   * @throws Exception
   */
  @Test
  public void testCleanShutdownOfVolume() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration config = new HdfsConfiguration();
      config.setLong(
          DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY, 1000);
      config.setTimeDuration(
          DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY, 0,
          TimeUnit.MILLISECONDS);
      config.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);

      cluster = new MiniDFSCluster.Builder(config).numDataNodes(1).build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      DataNode dataNode = cluster.getDataNodes().get(0);
      Path filePath = new Path("test.dat");
      // Create a file and keep the output stream unclosed.
      FSDataOutputStream out = fs.create(filePath, (short) 1);
      out.write(1);
      out.hflush();

      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, filePath);
      final FsVolumeImpl volume = (FsVolumeImpl) dataNode.getFSDataset().
          getVolume(block);
      File finalizedDir = volume.getFinalizedDir(cluster.getNamesystem()
          .getBlockPoolId());
      LocatedBlock lb = DFSTestUtil.getAllBlocks(fs, filePath).get(0);
      DatanodeInfo info = lb.getLocations()[0];

      if (finalizedDir.exists()) {
        // Remove write and execute access so that checkDiskErrorThread detects
        // this volume is bad.
        finalizedDir.setExecutable(false);
        finalizedDir.setWritable(false);
      }
      Assert.assertTrue("Reference count for the volume should be greater "
          + "than 0", volume.getReferenceCount() > 0);
      // Invoke the synchronous checkDiskError method
      dataNode.checkDiskError();
      // Sleep for 1 second so that datanode can interrupt and cluster clean up
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
          @Override public Boolean get() {
              return volume.getReferenceCount() == 0;
            }
          }, 100, 1000);
      assertThat(dataNode.getFSDataset().getNumFailedVolumes(), is(1));

      try {
        out.close();
        Assert.fail("This is not a valid code path. "
            + "out.close should have thrown an exception.");
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains(info.getXferAddr(), ioe);
      }
      finalizedDir.setWritable(true);
      finalizedDir.setExecutable(true);
    } finally {
    cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testReportBadBlocks() throws Exception {
    boolean threwException = false;
    MiniDFSCluster cluster = null;
    try {
      Configuration config = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(config).numDataNodes(1).build();
      cluster.waitActive();

      Assert.assertEquals(0, cluster.getNamesystem().getCorruptReplicaBlocks());
      DataNode dataNode = cluster.getDataNodes().get(0);
      ExtendedBlock block =
          new ExtendedBlock(cluster.getNamesystem().getBlockPoolId(), 0);
      try {
        // Test the reportBadBlocks when the volume is null
        dataNode.reportBadBlocks(block);
      } catch (NullPointerException npe) {
        threwException = true;
      }
      Thread.sleep(3000);
      Assert.assertFalse(threwException);
      Assert.assertEquals(0, cluster.getNamesystem().getCorruptReplicaBlocks());

      FileSystem fs = cluster.getFileSystem();
      Path filePath = new Path("testData");
      DFSTestUtil.createFile(fs, filePath, 1, (short) 1, 0);

      block = DFSTestUtil.getFirstBlock(fs, filePath);
      // Test for the overloaded method reportBadBlocks
      dataNode.reportBadBlocks(block, dataNode.getFSDataset()
          .getFsVolumeReferences().get(0));
      Thread.sleep(3000);
      BlockManagerTestUtil.updateState(cluster.getNamesystem()
          .getBlockManager());
      // Verify the bad block has been reported to namenode
      Assert.assertEquals(1, cluster.getNamesystem().getCorruptReplicaBlocks());
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testMoveBlockFailure() {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(1)
          .storageTypes(new StorageType[]{StorageType.DISK, StorageType.DISK})
          .storagesPerDatanode(2)
          .build();
      FileSystem fs = cluster.getFileSystem();
      DataNode dataNode = cluster.getDataNodes().get(0);

      Path filePath = new Path("testData");
      DFSTestUtil.createFile(fs, filePath, 100, (short) 1, 0);
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, filePath);

      FsDatasetImpl fsDataSetImpl = (FsDatasetImpl) dataNode.getFSDataset();
      ReplicaInfo newReplicaInfo = createNewReplicaObj(block, fsDataSetImpl);

      // Append to file to update its GS
      FSDataOutputStream out = fs.append(filePath, (short) 1);
      out.write(100);
      out.hflush();

      // Call finalizeNewReplica
      LOG.info("GenerationStamp of old replica: {}",
          block.getGenerationStamp());
      LOG.info("GenerationStamp of new replica: {}", fsDataSetImpl
          .getReplicaInfo(block.getBlockPoolId(), newReplicaInfo.getBlockId())
          .getGenerationStamp());
      LambdaTestUtils.intercept(IOException.class, "Generation Stamp "
              + "should be monotonically increased.",
          () -> fsDataSetImpl.finalizeNewReplica(newReplicaInfo, block));
    } catch (Exception ex) {
      LOG.info("Exception in testMoveBlockFailure ", ex);
      fail("Exception while testing testMoveBlockFailure ");
    } finally {
      if (cluster.isClusterUp()) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout = 30000)
  public void testMoveBlockSuccess() {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(1)
          .storageTypes(new StorageType[]{StorageType.DISK, StorageType.DISK})
          .storagesPerDatanode(2)
          .build();
      FileSystem fs = cluster.getFileSystem();
      DataNode dataNode = cluster.getDataNodes().get(0);

      Path filePath = new Path("testData");
      DFSTestUtil.createFile(fs, filePath, 100, (short) 1, 0);
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, filePath);

      FsDatasetImpl fsDataSetImpl = (FsDatasetImpl) dataNode.getFSDataset();
      ReplicaInfo newReplicaInfo = createNewReplicaObj(block, fsDataSetImpl);
      fsDataSetImpl.finalizeNewReplica(newReplicaInfo, block);

    } catch (Exception ex) {
      LOG.info("Exception in testMoveBlockSuccess ", ex);
      fail("MoveBlock operation should succeed");
    } finally {
      if (cluster.isClusterUp()) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Create a new temporary replica of replicaInfo object in another volume.
   *
   * @param block         - Extended Block
   * @param fsDataSetImpl - FsDatasetImpl reference
   * @throws IOException
   */
  private ReplicaInfo createNewReplicaObj(ExtendedBlock block, FsDatasetImpl
      fsDataSetImpl) throws IOException {
    ReplicaInfo replicaInfo = fsDataSetImpl.getReplicaInfo(block);
    FsVolumeSpi destVolume = null;

    final String srcStorageId = fsDataSetImpl.getVolume(block).getStorageID();
    try (FsVolumeReferences volumeReferences =
        fsDataSetImpl.getFsVolumeReferences()) {
      for (int i = 0; i < volumeReferences.size(); i++) {
        if (!volumeReferences.get(i).getStorageID().equals(srcStorageId)) {
          destVolume = volumeReferences.get(i);
          break;
        }
      }
    }
    return fsDataSetImpl.copyReplicaToVolume(block, replicaInfo,
        destVolume.obtainReference());
  }

}