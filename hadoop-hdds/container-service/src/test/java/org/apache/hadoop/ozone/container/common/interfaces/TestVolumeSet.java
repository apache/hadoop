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

package org.apache.hadoop.ozone.container.common.interfaces;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ozone.container.common.impl.VolumeInfo;
import org.apache.hadoop.ozone.container.common.impl.VolumeSet;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link VolumeSet} operations.
 */
public class TestVolumeSet {

  private OzoneConfiguration conf;
  protected VolumeSet volumeSet;
  protected final String baseDir = MiniDFSCluster.getBaseDirectory();
  protected final String volume1 = baseDir + "disk1";
  protected final String volume2 = baseDir + "disk2";
  private final List<String> volumes = new ArrayList<>();

  private void initializeVolumeSet() throws Exception {
    volumeSet = new VolumeSet(conf);
  }

  @Rule
  public Timeout testTimeout = new Timeout(300_000);

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    String dataDirKey = volume1 + "," + volume2;
    volumes.add(volume1);
    volumes.add(volume2);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDirKey);
    initializeVolumeSet();
  }

  @Test
  public void testVolumeSetInitialization() throws Exception {

    List<VolumeInfo> volumesList = volumeSet.getVolumesList();

    // VolumeSet initialization should add volume1 and volume2 to VolumeSet
    assertEquals("VolumeSet intialization is incorrect",
        volumesList.size(), volumes.size());
    assertEquals(volume1, volumesList.get(0).getRootDir().toString());
    assertEquals(volume2, volumesList.get(1).getRootDir().toString());
  }

  @Test
  public void testAddVolume() throws Exception {

    List<VolumeInfo> volumesList = volumeSet.getVolumesList();
    assertEquals(2, volumeSet.getVolumesList().size());

    // Add a volume to VolumeSet
    String volume3 = baseDir + "disk3";
    volumeSet.addVolume(volume3);

    assertEquals(3, volumeSet.getVolumesList().size());
    assertEquals("AddVolume did not add requested volume to VolumeSet",
        volume3,
        volumeSet.getVolumesList().get(2).getRootDir().toString());
  }

  @Test
  public void testFailVolume() throws Exception {

    //Fail a volume
    volumeSet.failVolume(volume1);

    // Failed volume should not show up in the volumeList
    assertEquals(1, volumeSet.getVolumesList().size());

    // Failed volume should be added to FailedVolumeList
    assertEquals("Failed volume not present in FailedVolumeList",
        1, volumeSet.getFailedVolumesList().size());
    assertEquals("Failed Volume list did not match", volume1,
        volumeSet.getFailedVolumesList().get(0).getRootDir().toString());

    // Failed volume should exist in VolumeMap with isFailed flag set to true
    Path volume1Path = new Path(volume1);
    assertTrue(volumeSet.getVolumeMap().containsKey(volume1Path));
    assertTrue(volumeSet.getVolumeMap().get(volume1Path).isFailed());
  }

  @Test
  public void testRemoveVolume() throws Exception {

    List<VolumeInfo> volumesList = volumeSet.getVolumesList();
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
    String expectedLogMessage = "Volume: " + volume1 + " does not exist in "
        + "volumeMap.";
    assertTrue("Log output does not contain expected log message: "
        + expectedLogMessage, logs.getOutput().contains(expectedLogMessage));
  }
}
