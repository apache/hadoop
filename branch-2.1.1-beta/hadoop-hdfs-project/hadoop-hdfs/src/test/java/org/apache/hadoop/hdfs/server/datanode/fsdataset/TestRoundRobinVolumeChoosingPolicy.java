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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestRoundRobinVolumeChoosingPolicy {

  // Test the Round-Robin block-volume choosing algorithm.
  @Test
  public void testRR() throws Exception {
    @SuppressWarnings("unchecked")
    final RoundRobinVolumeChoosingPolicy<FsVolumeSpi> policy = 
        ReflectionUtils.newInstance(RoundRobinVolumeChoosingPolicy.class, null);
    testRR(policy);
  }
  
  public static void testRR(VolumeChoosingPolicy<FsVolumeSpi> policy)
      throws Exception {
    final List<FsVolumeSpi> volumes = new ArrayList<FsVolumeSpi>();

    // First volume, with 100 bytes of space.
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(0).getAvailable()).thenReturn(100L);

    // Second volume, with 200 bytes of space.
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(1).getAvailable()).thenReturn(200L);
    
    // Test two rounds of round-robin choosing
    Assert.assertEquals(volumes.get(0), policy.chooseVolume(volumes, 0));
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 0));
    Assert.assertEquals(volumes.get(0), policy.chooseVolume(volumes, 0));
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 0));

    // The first volume has only 100L space, so the policy should
    // wisely choose the second one in case we ask for more.
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 150));

    // Fail if no volume can be chosen?
    try {
      policy.chooseVolume(volumes, Long.MAX_VALUE);
      Assert.fail();
    } catch (IOException e) {
      // Passed.
    }
  }
  
  // ChooseVolume should throw DiskOutOfSpaceException
  // with volume and block sizes in exception message.
  @Test
  public void testRRPolicyExceptionMessage() throws Exception {
    final RoundRobinVolumeChoosingPolicy<FsVolumeSpi> policy
        = new RoundRobinVolumeChoosingPolicy<FsVolumeSpi>();
    testRRPolicyExceptionMessage(policy);
  }
  
  public static void testRRPolicyExceptionMessage(
      VolumeChoosingPolicy<FsVolumeSpi> policy) throws Exception {
    final List<FsVolumeSpi> volumes = new ArrayList<FsVolumeSpi>();

    // First volume, with 500 bytes of space.
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(0).getAvailable()).thenReturn(500L);

    // Second volume, with 600 bytes of space.
    volumes.add(Mockito.mock(FsVolumeSpi.class));
    Mockito.when(volumes.get(1).getAvailable()).thenReturn(600L);

    int blockSize = 700;
    try {
      policy.chooseVolume(volumes, blockSize);
      Assert.fail("expected to throw DiskOutOfSpaceException");
    } catch(DiskOutOfSpaceException e) {
      Assert.assertEquals("Not returnig the expected message",
          "Out of space: The volume with the most available space (=" + 600
              + " B) is less than the block size (=" + blockSize + " B).",
          e.getMessage());
    }
  }

}
