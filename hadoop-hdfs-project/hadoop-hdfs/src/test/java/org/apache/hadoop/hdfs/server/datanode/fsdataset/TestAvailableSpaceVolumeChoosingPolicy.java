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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAvailableSpaceVolumeChoosingPolicy {
  
  private static final int RANDOMIZED_ITERATIONS = 10000;
  private static final float RANDOMIZED_ERROR_PERCENT = 0.05f;
  private static final long RANDOMIZED_ALLOWED_ERROR = (long) (RANDOMIZED_ERROR_PERCENT * RANDOMIZED_ITERATIONS);
  
  private static void initPolicy(VolumeChoosingPolicy<FsVolumeSpi> policy,
      float preferencePercent) {
    Configuration conf = new Configuration();
    // Set the threshold to consider volumes imbalanced to 1MB
    conf.setLong(
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_KEY,
        1024 * 1024); // 1MB
    conf.setFloat(
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
        preferencePercent);
    ((Configurable) policy).setConf(conf);
  }
  
  // Test the Round-Robin block-volume fallback path when all volumes are within
  // the threshold.
  @Test(timeout=60000)
  public void testRR() throws Exception {
    @SuppressWarnings("unchecked")
    final AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = 
        ReflectionUtils.newInstance(AvailableSpaceVolumeChoosingPolicy.class, null);
    initPolicy(policy, 1.0f);
    TestRoundRobinVolumeChoosingPolicy.testRR(policy);
  }
  
  // ChooseVolume should throw DiskOutOfSpaceException
  // with volume and block sizes in exception message.
  @Test(timeout=60000)
  public void testRRPolicyExceptionMessage() throws Exception {
    final AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy
        = new AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi>();
    initPolicy(policy, 1.0f);
    TestRoundRobinVolumeChoosingPolicy.testRRPolicyExceptionMessage(policy);
  }
  
  @Test(timeout=60000)
  public void testTwoUnbalancedVolumes() throws Exception {
    @SuppressWarnings("unchecked")
    final AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = 
        ReflectionUtils.newInstance(AvailableSpaceVolumeChoosingPolicy.class, null);
    initPolicy(policy, 1.0f);
    
    List<FsVolumeSpi> volumes = new ArrayList<FsVolumeSpi>();
    
    // First volume with 1MB free space
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(0).getAvailable()).thenReturn(1024L * 1024L);
    
    // Second volume with 3MB free space, which is a difference of 2MB, more
    // than the threshold of 1MB.
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(1).getAvailable()).thenReturn(1024L * 1024L * 3);
    
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 100));
  }
  
  @Test(timeout=60000)
  public void testThreeUnbalancedVolumes() throws Exception {
    @SuppressWarnings("unchecked")
    final AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = 
        ReflectionUtils.newInstance(AvailableSpaceVolumeChoosingPolicy.class, null);
    
    List<FsVolumeSpi> volumes = new ArrayList<FsVolumeSpi>();
    
    // First volume with 1MB free space
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(0).getAvailable()).thenReturn(1024L * 1024L);
    
    // Second volume with 3MB free space, which is a difference of 2MB, more
    // than the threshold of 1MB.
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(1).getAvailable()).thenReturn(1024L * 1024L * 3);
    
    // Third volume, again with 3MB free space.
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(2).getAvailable()).thenReturn(1024L * 1024L * 3);
    
    // We should alternate assigning between the two volumes with a lot of free
    // space.
    initPolicy(policy, 1.0f);
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(2), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(2), policy.chooseVolume(volumes, 100));

    // All writes should be assigned to the volume with the least free space.
    initPolicy(policy, 0.0f);
    Assert.assertEquals(volumes.get(0), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(0), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(0), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(0), policy.chooseVolume(volumes, 100));
  }
  
  @Test(timeout=60000)
  public void testFourUnbalancedVolumes() throws Exception {
    @SuppressWarnings("unchecked")
    final AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = 
        ReflectionUtils.newInstance(AvailableSpaceVolumeChoosingPolicy.class, null);
    
    List<FsVolumeSpi> volumes = new ArrayList<FsVolumeSpi>();
    
    // First volume with 1MB free space
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(0).getAvailable()).thenReturn(1024L * 1024L);
    
    // Second volume with 1MB + 1 byte free space
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(1).getAvailable()).thenReturn(1024L * 1024L + 1);
    
    // Third volume with 3MB free space, which is a difference of 2MB, more
    // than the threshold of 1MB.
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(2).getAvailable()).thenReturn(1024L * 1024L * 3);
    
    // Fourth volume, again with 3MB free space.
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(3).getAvailable()).thenReturn(1024L * 1024L * 3);
    
    // We should alternate assigning between the two volumes with a lot of free
    // space.
    initPolicy(policy, 1.0f);
    Assert.assertEquals(volumes.get(2), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(3), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(2), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(3), policy.chooseVolume(volumes, 100));

    // We should alternate assigning between the two volumes with less free
    // space.
    initPolicy(policy, 0.0f);
    Assert.assertEquals(volumes.get(0), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(0), policy.chooseVolume(volumes, 100));
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 100));
  }
  
  @Test(timeout=60000)
  public void testNotEnoughSpaceOnSelectedVolume() throws Exception {
    @SuppressWarnings("unchecked")
    final AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = 
        ReflectionUtils.newInstance(AvailableSpaceVolumeChoosingPolicy.class, null);
    
    List<FsVolumeSpi> volumes = new ArrayList<FsVolumeSpi>();
    
    // First volume with 1MB free space
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(0).getAvailable()).thenReturn(1024L * 1024L);
    
    // Second volume with 3MB free space, which is a difference of 2MB, more
    // than the threshold of 1MB.
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(1).getAvailable()).thenReturn(1024L * 1024L * 3);
    
    // All writes should be assigned to the volume with the least free space.
    // However, if the volume with the least free space doesn't have enough
    // space to accept the replica size, and another volume does have enough
    // free space, that should be chosen instead.
    initPolicy(policy, 0.0f);
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 1024L * 1024L * 2));
  }
  
  @Test(timeout=60000)
  public void testAvailableSpaceChanges() throws Exception {
    @SuppressWarnings("unchecked")
    final AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = 
        ReflectionUtils.newInstance(AvailableSpaceVolumeChoosingPolicy.class, null);
    initPolicy(policy, 1.0f);
    
    List<FsVolumeSpi> volumes = new ArrayList<FsVolumeSpi>();
    
    // First volume with 1MB free space
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(0).getAvailable()).thenReturn(1024L * 1024L);
    
    // Second volume with 3MB free space, which is a difference of 2MB, more
    // than the threshold of 1MB.
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(1).getAvailable())
        .thenReturn(1024L * 1024L * 3)
        .thenReturn(1024L * 1024L * 3)
        .thenReturn(1024L * 1024L * 3)
        .thenReturn(1024L * 1024L * 1); // After the third check, return 1MB.
    
    // Should still be able to get a volume for the replica even though the
    // available space on the second volume changed.
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 100));
  }
  
  @Test(timeout=60000)
  public void randomizedTest1() throws Exception {
    doRandomizedTest(0.75f, 1, 1);
  }
  
  @Test(timeout=60000)
  public void randomizedTest2() throws Exception {
    doRandomizedTest(0.75f, 5, 1);
  }
  
  @Test(timeout=60000)
  public void randomizedTest3() throws Exception {
    doRandomizedTest(0.75f, 1, 5);
  }
  
  @Test(timeout=60000)
  public void randomizedTest4() throws Exception {
    doRandomizedTest(0.90f, 5, 1);
  }
  
  /*
   * Ensure that we randomly select the lesser-used volumes with appropriate
   * frequency.
   */
  public void doRandomizedTest(float preferencePercent, int lowSpaceVolumes,
      int highSpaceVolumes) throws Exception {
    Random random = new Random(123L);
    final AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy =
        new AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi>(random);

    List<FsVolumeSpi> volumes = new ArrayList<FsVolumeSpi>();
    
    // Volumes with 1MB free space
    for (int i = 0; i < lowSpaceVolumes; i++) {
      FsVolumeSpi volume = Mockito.mock(FsVolumeSpi.class);
      Mockito.when(volume.getAvailable()).thenReturn(1024L * 1024L);
      volumes.add(volume);
    }
    
    // Volumes with 3MB free space
    for (int i = 0; i < highSpaceVolumes; i++) {
      FsVolumeSpi volume = Mockito.mock(FsVolumeSpi.class);
      Mockito.when(volume.getAvailable()).thenReturn(1024L * 1024L * 3);
      volumes.add(volume);
    }
    
    initPolicy(policy, preferencePercent);
    long lowAvailableSpaceVolumeSelected = 0;
    long highAvailableSpaceVolumeSelected = 0;
    for (int i = 0; i < RANDOMIZED_ITERATIONS; i++) {
      FsVolumeSpi volume = policy.chooseVolume(volumes, 100);
      for (int j = 0; j < volumes.size(); j++) {
        // Note how many times the first low available volume was selected
        if (volume == volumes.get(j) && j == 0) {
          lowAvailableSpaceVolumeSelected++;
        }
        // Note how many times the first high available volume was selected
        if (volume == volumes.get(j) && j == lowSpaceVolumes) {
          highAvailableSpaceVolumeSelected++;
          break;
        }
      }
    }
    
    // Calculate the expected ratio of how often low available space volumes
    // were selected vs. high available space volumes.
    float expectedSelectionRatio = preferencePercent / (1 - preferencePercent);
    
    GenericTestUtils.assertValueNear(
        (long)(lowAvailableSpaceVolumeSelected * expectedSelectionRatio),
        highAvailableSpaceVolumeSelected,
        RANDOMIZED_ALLOWED_ERROR);
  }
  
}
