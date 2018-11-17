/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link HddsVolume}.
 */
public class TestHddsVolume {

  private static final String DATANODE_UUID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final Configuration CONF = new Configuration();
  private static final String DU_CACHE_FILE = "scmUsed";
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private File rootDir;
  private HddsVolume volume;
  private File versionFile;

  @Before
  public void setup() throws Exception {
    rootDir = new File(folder.getRoot(), HddsVolume.HDDS_VOLUME_DIR);
    volume = new HddsVolume.Builder(folder.getRoot().getPath())
        .datanodeUuid(DATANODE_UUID)
        .conf(CONF)
        .build();
    versionFile = HddsVolumeUtil.getVersionFile(rootDir);
  }

  @Test
  public void testHddsVolumeInitialization() throws Exception {

    // The initial state of HddsVolume should be "NOT_FORMATTED" when
    // clusterID is not specified and the version file should not be written
    // to disk.
    assertTrue(volume.getClusterID() == null);
    assertEquals(StorageType.DEFAULT, volume.getStorageType());
    assertEquals(HddsVolume.VolumeState.NOT_FORMATTED,
        volume.getStorageState());
    assertFalse("Version file should not be created when clusterID is not " +
        "known.", versionFile.exists());


    // Format the volume with clusterID.
    volume.format(CLUSTER_ID);

    // The state of HddsVolume after formatting with clusterID should be
    // NORMAL and the version file should exist.
    assertTrue("Volume format should create Version file",
        versionFile.exists());
    assertEquals(volume.getClusterID(), CLUSTER_ID);
    assertEquals(HddsVolume.VolumeState.NORMAL, volume.getStorageState());
  }

  @Test
  public void testReadPropertiesFromVersionFile() throws Exception {
    volume.format(CLUSTER_ID);

    Properties properties = DatanodeVersionFile.readFrom(versionFile);

    String storageID = HddsVolumeUtil.getStorageID(properties, versionFile);
    String clusterID = HddsVolumeUtil.getClusterID(
        properties, versionFile, CLUSTER_ID);
    String datanodeUuid = HddsVolumeUtil.getDatanodeUUID(
        properties, versionFile, DATANODE_UUID);
    long cTime = HddsVolumeUtil.getCreationTime(
        properties, versionFile);
    int layoutVersion = HddsVolumeUtil.getLayOutVersion(
        properties, versionFile);

    assertEquals(volume.getStorageID(), storageID);
    assertEquals(volume.getClusterID(), clusterID);
    assertEquals(volume.getDatanodeUuid(), datanodeUuid);
    assertEquals(volume.getCTime(), cTime);
    assertEquals(volume.getLayoutVersion(), layoutVersion);
  }

  @Test
  public void testShutdown() throws Exception {
    // Return dummy value > 0 for scmUsage so that scm cache file is written
    // during shutdown.
    GetSpaceUsed scmUsageMock = Mockito.mock(GetSpaceUsed.class);
    volume.setScmUsageForTesting(scmUsageMock);
    Mockito.when(scmUsageMock.getUsed()).thenReturn(Long.valueOf(100));

    assertTrue("Available volume should be positive",
        volume.getAvailable() > 0);

    // Shutdown the volume.
    volume.shutdown();

    // Volume state should be "NON_EXISTENT" when volume is shutdown.
    assertEquals(HddsVolume.VolumeState.NON_EXISTENT, volume.getStorageState());

    // Volume should save scmUsed cache file once volume is shutdown
    File scmUsedFile = new File(folder.getRoot(), DU_CACHE_FILE);
    System.out.println("scmUsedFile: " + scmUsedFile);
    assertTrue("scmUsed cache file should be saved on shutdown",
        scmUsedFile.exists());

    try {
      // Volume.getAvailable() should fail with IOException
      // as usage thread is shutdown.
      volume.getAvailable();
      fail("HddsVolume#shutdown test failed");
    } catch (Exception ex) {
      assertTrue(ex instanceof IOException);
      assertTrue(ex.getMessage().contains(
          "Volume Usage thread is not running."));
    }
  }
}
