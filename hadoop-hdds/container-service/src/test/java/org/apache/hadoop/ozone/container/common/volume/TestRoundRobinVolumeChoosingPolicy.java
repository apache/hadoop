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

import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Tests {@link RoundRobinVolumeChoosingPolicy}.
 */
public class TestRoundRobinVolumeChoosingPolicy {

  private RoundRobinVolumeChoosingPolicy policy;
  private List<HddsVolume> volumes;
  private VolumeSet volumeSet;

  private final String baseDir = MiniDFSCluster.getBaseDirectory();
  private final String volume1 = baseDir + "disk1";
  private final String volume2 = baseDir + "disk2";

  private static final String DUMMY_IP_ADDR = "0.0.0.0";

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String dataDirKey = volume1 + "," + volume2;
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDirKey);
    policy = ReflectionUtils.newInstance(
        RoundRobinVolumeChoosingPolicy.class, null);
    volumeSet = new VolumeSet(UUID.randomUUID().toString(), conf);
    volumes = volumeSet.getVolumesList();
  }

  @After
  public void cleanUp() {
    if (volumeSet != null) {
      volumeSet.shutdown();
      volumeSet = null;
    }
  }

  @Test
  public void testRRVolumeChoosingPolicy() throws Exception {
    HddsVolume hddsVolume1 = volumes.get(0);
    HddsVolume hddsVolume2 = volumes.get(1);

    // Set available space in volume1 to 100L
    setAvailableSpace(hddsVolume1, 100L);

    // Set available space in volume1 to 200L
    setAvailableSpace(hddsVolume2, 200L);

    Assert.assertEquals(100L, hddsVolume1.getAvailable());
    Assert.assertEquals(200L, hddsVolume2.getAvailable());

    // Test two rounds of round-robin choosing
    Assert.assertEquals(hddsVolume1, policy.chooseVolume(volumes, 0));
    Assert.assertEquals(hddsVolume2, policy.chooseVolume(volumes, 0));
    Assert.assertEquals(hddsVolume1, policy.chooseVolume(volumes, 0));
    Assert.assertEquals(hddsVolume2, policy.chooseVolume(volumes, 0));

    // The first volume has only 100L space, so the policy should
    // choose the second one in case we ask for more.
    Assert.assertEquals(hddsVolume2,
        policy.chooseVolume(volumes, 150));

    // Fail if no volume has enough space available
    try {
      policy.chooseVolume(volumes, Long.MAX_VALUE);
      Assert.fail();
    } catch (IOException e) {
      // Passed.
    }
  }

  @Test
  public void testRRPolicyExceptionMessage() throws Exception {
    HddsVolume hddsVolume1 = volumes.get(0);
    HddsVolume hddsVolume2 = volumes.get(1);

    // Set available space in volume1 to 100L
    setAvailableSpace(hddsVolume1, 100L);

    // Set available space in volume1 to 200L
    setAvailableSpace(hddsVolume2, 200L);

    int blockSize = 300;
    try {
      policy.chooseVolume(volumes, blockSize);
      Assert.fail("expected to throw DiskOutOfSpaceException");
    } catch(DiskOutOfSpaceException e) {
      Assert.assertEquals("Not returnig the expected message",
          "Out of space: The volume with the most available space (=" + 200
              + " B) is less than the container size (=" + blockSize + " B).",
          e.getMessage());
    }
  }

  private void setAvailableSpace(HddsVolume hddsVolume, long availableSpace)
      throws IOException {
    GetSpaceUsed scmUsageMock = Mockito.mock(GetSpaceUsed.class);
    hddsVolume.setScmUsageForTesting(scmUsageMock);
    // Set used space to capacity -requiredAvailableSpace so that
    // getAvailable() returns us the specified availableSpace.
    Mockito.when(scmUsageMock.getUsed()).thenReturn(
        (hddsVolume.getCapacity() - availableSpace));
  }
}
