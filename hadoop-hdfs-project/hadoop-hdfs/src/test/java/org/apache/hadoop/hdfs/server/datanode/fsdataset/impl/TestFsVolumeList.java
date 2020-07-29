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

import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestFsVolumeList {

  private Configuration conf;
  private VolumeChoosingPolicy<FsVolumeImpl> blockChooser =
      new RoundRobinVolumeChoosingPolicy<>();
  private FsDatasetImpl dataset = null;
  private String baseDir;
  private BlockScanner blockScanner;

  @Before
  public void setUp() {
    dataset = mock(FsDatasetImpl.class);
    baseDir = new FileSystemTestHelper().getTestRootDir();
    Configuration blockScannerConf = new Configuration();
    blockScannerConf.setInt(DFSConfigKeys.
        DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);
    blockScanner = new BlockScanner(null, blockScannerConf);
    conf = new Configuration();
  }

  @Test(timeout=30000)
  public void testGetNextVolumeWithClosedVolume() throws IOException {
    FsVolumeList volumeList = new FsVolumeList(
        Collections.<VolumeFailureInfo>emptyList(), blockScanner, blockChooser);
    final List<FsVolumeImpl> volumes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      File curDir = new File(baseDir, "nextvolume-" + i);
      curDir.mkdirs();
      FsVolumeImpl volume = new FsVolumeImplBuilder()
          .setConf(conf)
          .setDataset(dataset)
          .setStorageID("storage-id")
          .setStorageDirectory(
              new StorageDirectory(StorageLocation.parse(curDir.getPath())))
          .build();
      volume.setCapacityForTesting(1024 * 1024 * 1024);
      volumes.add(volume);
      volumeList.addVolume(volume.obtainReference());
    }

    // Close the second volume.
    volumes.get(1).setClosed();
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return volumes.get(1).checkClosed();
        }
      }, 100, 3000);
    } catch (TimeoutException e) {
      fail("timed out while waiting for volume to be removed.");
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    for (int i = 0; i < 10; i++) {
      try (FsVolumeReference ref =
          volumeList.getNextVolume(StorageType.DEFAULT, null, 128)) {
        // volume No.2 will not be chosen.
        assertNotEquals(ref.getVolume(), volumes.get(1));
      }
    }
  }

  @Test(timeout=30000)
  public void testReleaseVolumeRefIfNoBlockScanner() throws IOException {
    FsVolumeList volumeList = new FsVolumeList(
        Collections.<VolumeFailureInfo>emptyList(), null, blockChooser);
    File volDir = new File(baseDir, "volume-0");
    volDir.mkdirs();
    FsVolumeImpl volume = new FsVolumeImplBuilder()
        .setConf(conf)
        .setDataset(dataset)
        .setStorageID("storage-id")
        .setStorageDirectory(
            new StorageDirectory(StorageLocation.parse(volDir.getPath())))
        .build();
    FsVolumeReference ref = volume.obtainReference();
    volumeList.addVolume(ref);
    assertNull(ref.getVolume());
  }

  @Test
  public void testDfsReservedForDifferentStorageTypes() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY, 100L);

    File volDir = new File(baseDir, "volume-0");
    volDir.mkdirs();
    // when storage type reserved is not configured,should consider
    // dfs.datanode.du.reserved.
    FsVolumeImpl volume = new FsVolumeImplBuilder().setDataset(dataset)
        .setStorageDirectory(
            new StorageDirectory(
                StorageLocation.parse("[RAM_DISK]"+volDir.getPath())))
        .setStorageID("storage-id")
        .setConf(conf)
        .build();
    assertEquals("", 100L, volume.getReserved());
    // when storage type reserved is configured.
    conf.setLong(
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY + "."
            + StringUtils.toLowerCase(StorageType.RAM_DISK.toString()), 1L);
    conf.setLong(
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY + "."
            + StringUtils.toLowerCase(StorageType.SSD.toString()), 2L);
    FsVolumeImpl volume1 = new FsVolumeImplBuilder().setDataset(dataset)
        .setStorageDirectory(
            new StorageDirectory(
                StorageLocation.parse("[RAM_DISK]"+volDir.getPath())))
        .setStorageID("storage-id")
        .setConf(conf)
        .build();
    assertEquals("", 1L, volume1.getReserved());
    FsVolumeImpl volume2 = new FsVolumeImplBuilder().setDataset(dataset)
        .setStorageDirectory(
            new StorageDirectory(
                StorageLocation.parse("[SSD]"+volDir.getPath())))
        .setStorageID("storage-id")
        .setConf(conf)
        .build();
    assertEquals("", 2L, volume2.getReserved());
    FsVolumeImpl volume3 = new FsVolumeImplBuilder().setDataset(dataset)
        .setStorageDirectory(
            new StorageDirectory(
                StorageLocation.parse("[DISK]"+volDir.getPath())))
        .setStorageID("storage-id")
        .setConf(conf)
        .build();
    assertEquals("", 100L, volume3.getReserved());
    FsVolumeImpl volume4 = new FsVolumeImplBuilder().setDataset(dataset)
        .setStorageDirectory(
            new StorageDirectory(
                StorageLocation.parse(volDir.getPath())))
        .setStorageID("storage-id")
        .setConf(conf)
        .build();
    assertEquals("", 100L, volume4.getReserved());
  }

  @Test
  public void testNonDfsUsedMetricForVolume() throws Exception {
    File volDir = new File(baseDir, "volume-0");
    volDir.mkdirs();
    /*
     * Lets have the example.
     * Capacity - 1000
     * Reserved - 100G
     * DfsUsed  - 200
     * Actual Non-DfsUsed - 300 -->(expected)
     * ReservedForReplicas - 50
     */
    long diskCapacity = 1000L;
    long duReserved = 100L;
    long dfsUsage = 200L;
    long actualNonDfsUsage = 300L;
    long reservedForReplicas = 50L;
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY, duReserved);
    FsVolumeImpl volume = new FsVolumeImplBuilder().setDataset(dataset)
        .setStorageDirectory(
            new StorageDirectory(
                StorageLocation.parse(volDir.getPath())))
        .setStorageID("storage-id")
        .setConf(conf)
        .build();
    FsVolumeImpl spyVolume = Mockito.spy(volume);
    // Set Capacity for testing
    long testCapacity = diskCapacity - duReserved;
    spyVolume.setCapacityForTesting(testCapacity);
    // Mock volume.getDfAvailable()
    long dfAvailable = diskCapacity - dfsUsage - actualNonDfsUsage;
    Mockito.doReturn(dfAvailable).when(spyVolume).getDfAvailable();
    // Mock dfsUsage
    Mockito.doReturn(dfsUsage).when(spyVolume).getDfsUsed();
    // Mock reservedForReplcas
    Mockito.doReturn(reservedForReplicas).when(spyVolume)
        .getReservedForReplicas();
    Mockito.doReturn(actualNonDfsUsage).when(spyVolume)
        .getActualNonDfsUsed();
    long expectedNonDfsUsage =
        actualNonDfsUsage - duReserved;
    assertEquals(expectedNonDfsUsage, spyVolume.getNonDfsUsed());
  }

  @Test
  public void testDfsReservedPercentageForDifferentStorageTypes()
      throws IOException {
    conf.setClass(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY,
        ReservedSpaceCalculator.ReservedSpaceCalculatorPercentage.class,
        ReservedSpaceCalculator.class);
    conf.setLong(DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY, 15);

    File volDir = new File(baseDir, "volume-0");
    volDir.mkdirs();

    DF usage = mock(DF.class);
    when(usage.getCapacity()).thenReturn(4000L);
    when(usage.getAvailable()).thenReturn(1000L);

    // when storage type reserved is not configured, should consider
    // dfs.datanode.du.reserved.pct
    FsVolumeImpl volume = new FsVolumeImplBuilder()
        .setConf(conf)
        .setDataset(dataset)
        .setStorageID("storage-id")
        .setStorageDirectory(
            new StorageDirectory(StorageLocation.parse(
                "[RAM_DISK]" + volDir.getPath())))
        .setUsage(usage)
        .build();

    assertEquals(600, volume.getReserved());
    assertEquals(3400, volume.getCapacity());
    assertEquals(400, volume.getAvailable());

    // when storage type reserved is configured.
    conf.setLong(
        DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY + "."
            + StringUtils.toLowerCase(StorageType.RAM_DISK.toString()), 10);
    conf.setLong(
        DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY + "."
            + StringUtils.toLowerCase(StorageType.SSD.toString()), 50);
    FsVolumeImpl volume1 = new FsVolumeImplBuilder()
        .setConf(conf)
        .setDataset(dataset)
        .setStorageID("storage-id")
        .setStorageDirectory(
            new StorageDirectory(StorageLocation.parse(
                "[RAM_DISK]" + volDir.getPath())))
        .setUsage(usage)
        .build();
    assertEquals(400, volume1.getReserved());
    assertEquals(3600, volume1.getCapacity());
    assertEquals(600, volume1.getAvailable());
    FsVolumeImpl volume2 = new FsVolumeImplBuilder()
        .setConf(conf)
        .setDataset(dataset)
        .setStorageID("storage-id")
        .setStorageDirectory(
            new StorageDirectory(StorageLocation.parse(
                "[SSD]" + volDir.getPath())))
        .setUsage(usage)
        .build();
    assertEquals(2000, volume2.getReserved());
    assertEquals(2000, volume2.getCapacity());
    assertEquals(0, volume2.getAvailable());
    FsVolumeImpl volume3 = new FsVolumeImplBuilder()
        .setConf(conf)
        .setDataset(dataset)
        .setStorageID("storage-id")
        .setStorageDirectory(
            new StorageDirectory(StorageLocation.parse(
                "[DISK]" + volDir.getPath())))
        .setUsage(usage)
        .build();
    assertEquals(600, volume3.getReserved());
    FsVolumeImpl volume4 = new FsVolumeImplBuilder()
        .setConf(conf)
        .setDataset(dataset)
        .setStorageID("storage-id")
        .setStorageDirectory(
            new StorageDirectory(StorageLocation.parse(volDir.getPath())))
        .setUsage(usage)
        .build();
    assertEquals(600, volume4.getReserved());
  }

  @Test(timeout = 60000)
  public void testAddRplicaProcessorForAddingReplicaInMap() throws Exception {
    BlockPoolSlice.reInitializeAddReplicaThreadPool();
    Configuration cnf = new Configuration();
    int poolSize = 5;
    cnf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    cnf.setInt(
        DFSConfigKeys.DFS_DATANODE_VOLUMES_REPLICA_ADD_THREADPOOL_SIZE_KEY,
        poolSize);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(cnf).numDataNodes(1)
        .storagesPerDatanode(1).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    // Generate data blocks.
    ExecutorService pool = Executors.newFixedThreadPool(10);
    List<Future<?>> futureList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Thread thread = new Thread() {
        @Override
        public void run() {
          for (int j = 0; j < 10; j++) {
            try {
              DFSTestUtil.createFile(fs, new Path("File_" + getName() + j), 10,
                  (short) 1, 0);
            } catch (IllegalArgumentException | IOException e) {
              e.printStackTrace();
            }
          }
        }
      };
      thread.setName("FileWriter" + i);
      futureList.add(pool.submit(thread));
    }
    // Wait for data generation
    for (Future<?> f : futureList) {
      f.get();
    }
    fs.close();
    FsDatasetImpl fsDataset = (FsDatasetImpl) cluster.getDataNodes().get(0)
        .getFSDataset();
    ReplicaMap volumeMap = new ReplicaMap(new ReentrantReadWriteLock());
    RamDiskReplicaTracker ramDiskReplicaMap = RamDiskReplicaTracker
        .getInstance(conf, fsDataset);
    FsVolumeImpl vol = (FsVolumeImpl) fsDataset.getFsVolumeReferences().get(0);
    String bpid = cluster.getNamesystem().getBlockPoolId();
    // It will create BlockPoolSlice.AddReplicaProcessor task's and lunch in
    // ForkJoinPool recursively
    vol.getVolumeMap(bpid, volumeMap, ramDiskReplicaMap);
    assertTrue("Failed to add all the replica to map", volumeMap.replicas(bpid)
        .size() == 1000);
    assertEquals("Fork pool should be initialize with configured pool size",
        poolSize, BlockPoolSlice.getAddReplicaForkPoolSize());
  }

  @Test(timeout = 60000)
  public void testInstanceOfAddReplicaThreadPool() throws Exception {
    // Start cluster with multiple namespace
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(
        new HdfsConfiguration())
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
        .numDataNodes(1).build()) {
      cluster.waitActive();
      FsDatasetImpl fsDataset = (FsDatasetImpl) cluster.getDataNodes().get(0)
          .getFSDataset();
      FsVolumeImpl vol = (FsVolumeImpl) fsDataset.getFsVolumeReferences()
          .get(0);
      ForkJoinPool threadPool1 = vol.getBlockPoolSlice(
          cluster.getNamesystem(0).getBlockPoolId()).getAddReplicaThreadPool();
      ForkJoinPool threadPool2 = vol.getBlockPoolSlice(
          cluster.getNamesystem(1).getBlockPoolId()).getAddReplicaThreadPool();
      assertEquals(
          "Thread pool instance should be same in all the BlockPoolSlice",
          threadPool1, threadPool2);
    }
  }

  @Test
  public void testGetCachedVolumeCapacity() throws IOException {
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_FIXED_VOLUME_SIZE_KEY,
        DFSConfigKeys.DFS_DATANODE_FIXED_VOLUME_SIZE_DEFAULT);

    long capacity = 4000L;
    DF usage = mock(DF.class);
    when(usage.getCapacity()).thenReturn(capacity);

    FsVolumeImpl volumeChanged = new FsVolumeImplBuilder()
        .setConf(conf)
        .setDataset(dataset)
        .setStorageID("storage-id")
        .setStorageDirectory(new StorageDirectory(StorageLocation.parse(
            "[RAM_DISK]volume-changed")))
        .setUsage(usage)
        .build();

    int callTimes = 5;
    for(int i = 0; i < callTimes; i++) {
      assertEquals(capacity, volumeChanged.getCapacity());
    }

    verify(usage, times(callTimes)).getCapacity();

    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_FIXED_VOLUME_SIZE_KEY, true);
    FsVolumeImpl volumeFixed = new FsVolumeImplBuilder()
        .setConf(conf)
        .setDataset(dataset)
        .setStorageID("storage-id")
        .setStorageDirectory(new StorageDirectory(StorageLocation.parse(
            "[RAM_DISK]volume-fixed")))
        .setUsage(usage)
        .build();

    for(int i = 0; i < callTimes; i++) {
      assertEquals(capacity, volumeFixed.getCapacity());
    }

    // reuse the capacity for fixed sized volume, only call one time
    // getCapacity of DF
    verify(usage, times(callTimes+1)).getCapacity();

    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_FIXED_VOLUME_SIZE_KEY,
        DFSConfigKeys.DFS_DATANODE_FIXED_VOLUME_SIZE_DEFAULT);
  }
}
