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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class TestFsVolumeList {

  private final Configuration conf = new Configuration();
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
  }

  @Test
  public void testGetNextVolumeWithClosedVolume() throws IOException {
    FsVolumeList volumeList = new FsVolumeList(
        Collections.<VolumeFailureInfo>emptyList(), blockScanner, blockChooser);
    List<FsVolumeImpl> volumes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      File curDir = new File(baseDir, "nextvolume-" + i);
      curDir.mkdirs();
      FsVolumeImpl volume = new FsVolumeImpl(dataset, "storage-id", curDir,
          conf, StorageType.DEFAULT);
      volume.setCapacityForTesting(1024 * 1024 * 1024);
      volumes.add(volume);
      volumeList.addVolume(volume.obtainReference());
    }

    // Close the second volume.
    volumes.get(1).closeAndWait();
    for (int i = 0; i < 10; i++) {
      try (FsVolumeReference ref =
          volumeList.getNextVolume(StorageType.DEFAULT, 128)) {
        // volume No.2 will not be chosen.
        assertNotEquals(ref.getVolume(), volumes.get(1));
      }
    }
  }

  @Test
  public void testCheckDirsWithClosedVolume() throws IOException {
    FsVolumeList volumeList = new FsVolumeList(
        Collections.<VolumeFailureInfo>emptyList(), blockScanner, blockChooser);
    List<FsVolumeImpl> volumes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      File curDir = new File(baseDir, "volume-" + i);
      curDir.mkdirs();
      FsVolumeImpl volume = new FsVolumeImpl(dataset, "storage-id", curDir,
          conf, StorageType.DEFAULT);
      volumes.add(volume);
      volumeList.addVolume(volume.obtainReference());
    }

    // Close the 2nd volume.
    volumes.get(1).closeAndWait();
    // checkDirs() should ignore the 2nd volume since it is closed.
    volumeList.checkDirs();
  }

  @Test
  public void testReleaseVolumeRefIfNoBlockScanner() throws IOException {
    FsVolumeList volumeList = new FsVolumeList(
        Collections.<VolumeFailureInfo>emptyList(), null, blockChooser);
    File volDir = new File(baseDir, "volume-0");
    volDir.mkdirs();
    FsVolumeImpl volume = new FsVolumeImpl(dataset, "storage-id", volDir,
        conf, StorageType.DEFAULT);
    FsVolumeReference ref = volume.obtainReference();
    volumeList.addVolume(ref);
    try {
      ref.close();
      fail("Should throw exception because the reference is closed in "
          + "VolumeList#addVolume().");
    } catch (IllegalStateException e) {
    }
  }
}
