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

package org.apache.hadoop.ozone.container.common.volume;

import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.volume.HddsVolume
    .HDDS_VOLUME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Tests {@link VolumeSet} operations.
 */
public class TestVolumeSet {

  private OzoneConfiguration conf;
  private VolumeSet volumeSet;
  private final String baseDir = MiniDFSCluster.getBaseDirectory();
  private final String volume1 = baseDir + "disk1";
  private final String volume2 = baseDir + "disk2";
  private final List<String> volumes = new ArrayList<>();

  private static final String DUMMY_IP_ADDR = "0.0.0.0";

  private void initializeVolumeSet() throws Exception {
    volumeSet = new VolumeSet(UUID.randomUUID().toString(), conf);
  }

  @Rule
  public Timeout testTimeout = new Timeout(300000);

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    String dataDirKey = volume1 + "," + volume2;
    volumes.add(volume1);
    volumes.add(volume2);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDirKey);
    initializeVolumeSet();
  }

  @After
  public void shutdown() throws IOException {
    // Delete the hdds volume root dir
    List<HddsVolume> hddsVolumes = new ArrayList<>();
    hddsVolumes.addAll(volumeSet.getVolumesList());
    hddsVolumes.addAll(volumeSet.getFailedVolumesList());

    for (HddsVolume volume : hddsVolumes) {
      FileUtils.deleteDirectory(volume.getHddsRootDir());
    }
    volumeSet.shutdown();

    FileUtil.fullyDelete(new File(baseDir));
  }

  private boolean checkVolumeExistsInVolumeSet(String volume) {
    for (HddsVolume hddsVolume : volumeSet.getVolumesList()) {
      if (hddsVolume.getHddsRootDir().getPath().equals(
          HddsVolumeUtil.getHddsRoot(volume))) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testVolumeSetInitialization() throws Exception {

    List<HddsVolume> volumesList = volumeSet.getVolumesList();

    // VolumeSet initialization should add volume1 and volume2 to VolumeSet
    assertEquals("VolumeSet intialization is incorrect",
        volumesList.size(), volumes.size());
    assertTrue("VolumeSet not initailized correctly",
        checkVolumeExistsInVolumeSet(volume1));
    assertTrue("VolumeSet not initailized correctly",
        checkVolumeExistsInVolumeSet(volume2));
  }

  @Test
  public void testAddVolume() {

    assertEquals(2, volumeSet.getVolumesList().size());

    // Add a volume to VolumeSet
    String volume3 = baseDir + "disk3";
    boolean success = volumeSet.addVolume(volume3);

    assertTrue(success);
    assertEquals(3, volumeSet.getVolumesList().size());
    assertTrue("AddVolume did not add requested volume to VolumeSet",
        checkVolumeExistsInVolumeSet(volume3));
  }

  @Test
  public void testFailVolume() throws Exception {

    //Fail a volume
    volumeSet.failVolume(volume1);

    // Failed volume should not show up in the volumeList
    assertEquals(1, volumeSet.getVolumesList().size());

    // Failed volume should be added to FailedVolumeList
    assertEquals("Failed volume not present in FailedVolumeMap",
        1, volumeSet.getFailedVolumesList().size());
    assertEquals("Failed Volume list did not match",
        HddsVolumeUtil.getHddsRoot(volume1),
        volumeSet.getFailedVolumesList().get(0).getHddsRootDir().getPath());
    assertTrue(volumeSet.getFailedVolumesList().get(0).isFailed());

    // Failed volume should not exist in VolumeMap
    assertFalse(volumeSet.getVolumeMap().containsKey(volume1));
  }

  @Test
  public void testRemoveVolume() throws Exception {

    assertEquals(2, volumeSet.getVolumesList().size());

    // Remove a volume from VolumeSet
    volumeSet.removeVolume(volume1);
    assertEquals(1, volumeSet.getVolumesList().size());

    // Attempting to remove a volume which does not exist in VolumeSet should
    // log a warning.
    LogCapturer logs = LogCapturer.captureLogs(
        LogFactory.getLog(VolumeSet.class));
    volumeSet.removeVolume(volume1);
    assertEquals(1, volumeSet.getVolumesList().size());
    String expectedLogMessage = "Volume : " +
        HddsVolumeUtil.getHddsRoot(volume1) + " does not exist in VolumeSet";
    assertTrue("Log output does not contain expected log message: "
        + expectedLogMessage, logs.getOutput().contains(expectedLogMessage));
  }

  @Test
  public void testVolumeInInconsistentState() throws Exception {
    assertEquals(2, volumeSet.getVolumesList().size());

    // Add a volume to VolumeSet
    String volume3 = baseDir + "disk3";

    // Create the root volume dir and create a sub-directory within it.
    File newVolume = new File(volume3, HDDS_VOLUME_DIR);
    System.out.println("new volume root: " + newVolume);
    newVolume.mkdirs();
    assertTrue("Failed to create new volume root", newVolume.exists());
    File dataDir = new File(newVolume, "chunks");
    dataDir.mkdirs();
    assertTrue(dataDir.exists());

    // The new volume is in an inconsistent state as the root dir is
    // non-empty but the version file does not exist. Add Volume should
    // return false.
    boolean success = volumeSet.addVolume(volume3);

    assertFalse(success);
    assertEquals(2, volumeSet.getVolumesList().size());
    assertTrue("AddVolume should fail for an inconsistent volume",
        !checkVolumeExistsInVolumeSet(volume3));

    // Delete volume3
    File volume = new File(volume3);
    FileUtils.deleteDirectory(volume);
  }

  @Test
  public void testShutdown() throws Exception {
    List<HddsVolume> volumesList = volumeSet.getVolumesList();

    volumeSet.shutdown();

    // Verify that the volumes are shutdown and the volumeUsage is set to null.
    for (HddsVolume volume : volumesList) {
      Assert.assertNull(volume.getVolumeInfo().getUsageForTesting());
      try {
        // getAvailable() should throw null pointer exception as usage is null.
        volume.getAvailable();
        fail("Volume shutdown failed.");
      } catch (IOException ex) {
        // Do Nothing. Exception is expected.
        assertTrue(ex.getMessage().contains(
            "Volume Usage thread is not running."));
      }
    }
  }

  @Test
  public void testFailVolumes() throws  Exception{
    VolumeSet volSet = null;
    File readOnlyVolumePath = new File(baseDir);
    //Set to readonly, so that this volume will be failed
    readOnlyVolumePath.setReadOnly();
    File volumePath = GenericTestUtils.getRandomizedTestDir();
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set(HDDS_DATANODE_DIR_KEY, readOnlyVolumePath.getAbsolutePath()
        + "," + volumePath.getAbsolutePath());
    volSet = new VolumeSet(UUID.randomUUID().toString(), ozoneConfig);
    assertTrue(volSet.getFailedVolumesList().size() == 1);
    assertEquals(readOnlyVolumePath, volSet.getFailedVolumesList().get(0)
        .getHddsRootDir());

    //Set back to writable
    try {
      readOnlyVolumePath.setWritable(true);
    } finally {
      FileUtil.fullyDelete(volumePath);
    }

  }
}
