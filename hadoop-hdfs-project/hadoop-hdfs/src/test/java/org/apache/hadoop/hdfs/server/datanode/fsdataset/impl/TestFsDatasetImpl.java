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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestFsDatasetImpl {
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

  private static Storage.StorageDirectory createStorageDirectory(File root) {
    Storage.StorageDirectory sd = new Storage.StorageDirectory(root);
    dsForStorageUuid.createStorageID(sd, false);
    return sd;
  }

  private static void createStorageDirs(DataStorage storage, Configuration conf,
      int numDirs) throws IOException {
    List<Storage.StorageDirectory> dirs =
        new ArrayList<Storage.StorageDirectory>();
    List<String> dirStrings = new ArrayList<String>();
    for (int i = 0; i < numDirs; i++) {
      File loc = new File(BASE_DIR + "/data" + i);
      dirStrings.add(loc.toString());
      loc.mkdirs();
      dirs.add(createStorageDirectory(loc));
      when(storage.getStorageDir(i)).thenReturn(dirs.get(i));
    }

    String dataDir = StringUtils.join(",", dirStrings);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDir);
    when(storage.getNumStorageDirs()).thenReturn(numDirs);
  }

  @Before
  public void setUp() throws IOException {
    datanode = mock(DataNode.class);
    storage = mock(DataStorage.class);
    this.conf = new Configuration();
    this.conf.setLong(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 0);
    final DNConf dnConf = new DNConf(conf);

    when(datanode.getConf()).thenReturn(conf);
    when(datanode.getDnConf()).thenReturn(dnConf);
    final BlockScanner disabledBlockScanner = new BlockScanner(datanode, conf);
    when(datanode.getBlockScanner()).thenReturn(disabledBlockScanner);

    createStorageDirs(storage, conf, NUM_INIT_VOLUMES);
    dataset = new FsDatasetImpl(datanode, storage, conf);
    for (String bpid : BLOCK_POOL_IDS) {
      dataset.addBlockPool(bpid, conf);
    }

    assertEquals(NUM_INIT_VOLUMES, dataset.getVolumes().size());
    assertEquals(0, dataset.getNumFailedVolumes());
  }

  @Test
  public void testAddVolumes() throws IOException {
    final int numNewVolumes = 3;
    final int numExistingVolumes = dataset.getVolumes().size();
    final int totalVolumes = numNewVolumes + numExistingVolumes;
    Set<String> expectedVolumes = new HashSet<String>();
    List<NamespaceInfo> nsInfos = Lists.newArrayList();
    for (String bpid : BLOCK_POOL_IDS) {
      nsInfos.add(new NamespaceInfo(0, CLUSTER_ID, bpid, 1));
    }
    for (int i = 0; i < numNewVolumes; i++) {
      String path = BASE_DIR + "/newData" + i;
      expectedVolumes.add(path);
      StorageLocation loc = StorageLocation.parse(path);
      Storage.StorageDirectory sd = createStorageDirectory(new File(path));
      DataStorage.VolumeBuilder builder =
          new DataStorage.VolumeBuilder(storage, sd);
      when(storage.prepareVolume(eq(datanode), eq(loc.getFile()),
          anyListOf(NamespaceInfo.class)))
          .thenReturn(builder);

      dataset.addVolume(loc, nsInfos);
    }

    assertEquals(totalVolumes, dataset.getVolumes().size());
    assertEquals(totalVolumes, dataset.storageMap.size());

    Set<String> actualVolumes = new HashSet<String>();
    for (int i = 0; i < numNewVolumes; i++) {
      actualVolumes.add(
          dataset.getVolumes().get(numExistingVolumes + i).getBasePath());
    }
    assertEquals(actualVolumes, expectedVolumes);
  }

  @Test(timeout = 30000)
  public void testRemoveVolumes() throws IOException {
    // Feed FsDataset with block metadata.
    final int NUM_BLOCKS = 100;
    for (int i = 0; i < NUM_BLOCKS; i++) {
      String bpid = BLOCK_POOL_IDS[NUM_BLOCKS % BLOCK_POOL_IDS.length];
      ExtendedBlock eb = new ExtendedBlock(bpid, i);
      try (ReplicaHandler replica =
          dataset.createRbw(StorageType.DEFAULT, eb, false)) {
      }
    }
    final String[] dataDirs =
        conf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY).split(",");
    final String volumePathToRemove = dataDirs[0];
    List<StorageLocation> volumesToRemove = new ArrayList<StorageLocation>();
    volumesToRemove.add(StorageLocation.parse(volumePathToRemove));

    dataset.removeVolumes(volumesToRemove);
    int expectedNumVolumes = dataDirs.length - 1;
    assertEquals("The volume has been removed from the volumeList.",
        expectedNumVolumes, dataset.getVolumes().size());
    assertEquals("The volume has been removed from the storageMap.",
        expectedNumVolumes, dataset.storageMap.size());

    try {
      dataset.asyncDiskService.execute(volumesToRemove.get(0).getFile(),
          new Runnable() {
            @Override
            public void run() {}
          });
      fail("Expect RuntimeException: the volume has been removed from the "
           + "AsyncDiskService.");
    } catch (RuntimeException e) {
      GenericTestUtils.assertExceptionContains("Cannot find root", e);
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
    final int numExistingVolumes = dataset.getVolumes().size();
    List<NamespaceInfo> nsInfos = new ArrayList<>();
    for (String bpid : BLOCK_POOL_IDS) {
      nsInfos.add(new NamespaceInfo(0, CLUSTER_ID, bpid, 1));
    }
    String newVolumePath = BASE_DIR + "/newVolumeToRemoveLater";
    StorageLocation loc = StorageLocation.parse(newVolumePath);

    Storage.StorageDirectory sd = createStorageDirectory(new File(newVolumePath));
    DataStorage.VolumeBuilder builder =
        new DataStorage.VolumeBuilder(storage, sd);
    when(storage.prepareVolume(eq(datanode), eq(loc.getFile()),
        anyListOf(NamespaceInfo.class)))
        .thenReturn(builder);

    dataset.addVolume(loc, nsInfos);
    assertEquals(numExistingVolumes + 1, dataset.getVolumes().size());

    when(storage.getNumStorageDirs()).thenReturn(numExistingVolumes + 1);
    when(storage.getStorageDir(numExistingVolumes)).thenReturn(sd);
    List<StorageLocation> volumesToRemove = Arrays.asList(loc);
    dataset.removeVolumes(volumesToRemove);
    assertEquals(numExistingVolumes, dataset.getVolumes().size());
  }

  @Test(timeout = 5000)
  public void testChangeVolumeWithRunningCheckDirs() throws IOException {
    RoundRobinVolumeChoosingPolicy<FsVolumeImpl> blockChooser =
        new RoundRobinVolumeChoosingPolicy<>();
    final BlockScanner blockScanner = new BlockScanner(datanode, conf);
    final FsVolumeList volumeList =
        new FsVolumeList(0, blockScanner, blockChooser);
    final List<FsVolumeImpl> oldVolumes = new ArrayList<>();

    // Initialize FsVolumeList with 5 mock volumes.
    final int NUM_VOLUMES = 5;
    for (int i = 0; i < NUM_VOLUMES; i++) {
      FsVolumeImpl volume = mock(FsVolumeImpl.class);
      oldVolumes.add(volume);
      when(volume.getBasePath()).thenReturn("data" + i);
      FsVolumeReference ref = mock(FsVolumeReference.class);
      when(ref.getVolume()).thenReturn(volume);
      volumeList.addVolume(ref);
    }

    // When call checkDirs() on the 2nd volume, anther "thread" removes the 5th
    // volume and add another volume. It does not affect checkDirs() running.
    final FsVolumeImpl newVolume = mock(FsVolumeImpl.class);
    final FsVolumeReference newRef = mock(FsVolumeReference.class);
    when(newRef.getVolume()).thenReturn(newVolume);
    FsVolumeImpl blockedVolume = volumeList.getVolumes().get(1);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        volumeList.removeVolume(new File("data4"));
        volumeList.addVolume(newRef);
        return null;
      }
    }).when(blockedVolume).checkDirs();

    FsVolumeImpl brokenVolume = volumeList.getVolumes().get(2);
    doThrow(new DiskChecker.DiskErrorException("broken"))
        .when(brokenVolume).checkDirs();

    volumeList.checkDirs();

    // Since FsVolumeImpl#checkDirs() get a snapshot of the list of volumes
    // before running removeVolume(), it is supposed to run checkDirs() on all
    // the old volumes.
    for (FsVolumeImpl volume : oldVolumes) {
      verify(volume).checkDirs();
    }
    // New volume is not visible to checkDirs() process.
    verify(newVolume, never()).checkDirs();
    assertTrue(volumeList.getVolumes().contains(newVolume));
    assertFalse(volumeList.getVolumes().contains(brokenVolume));
    assertEquals(NUM_VOLUMES - 1, volumeList.getVolumes().size());
  }
}
