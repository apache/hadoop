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

package org.apache.hadoop.ozone.container.common.impl;

import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link RoundRobinVolumeChoosingPolicy}.
 */
public class TestRoundRobinVolumeChoosingPolicy {

  private RoundRobinVolumeChoosingPolicy policy;

  @Before
  public void setup() {
   policy = ReflectionUtils.newInstance(
       RoundRobinVolumeChoosingPolicy.class, null);
  }

  @Test
  public void testRRVolumeChoosingPolicy() throws Exception {
    final List<VolumeInfo> volumes = new ArrayList<>();

    // First volume, with 100 bytes of space.
    volumes.add(Mockito.mock(VolumeInfo.class));
    Mockito.when(volumes.get(0).getAvailable()).thenReturn(100L);

    // Second volume, with 200 bytes of space.
    volumes.add(Mockito.mock(VolumeInfo.class));
    Mockito.when(volumes.get(1).getAvailable()).thenReturn(200L);

    // Test two rounds of round-robin choosing
    Assert.assertEquals(volumes.get(0), policy.chooseVolume(volumes, 0));
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 0));
    Assert.assertEquals(volumes.get(0), policy.chooseVolume(volumes, 0));
    Assert.assertEquals(volumes.get(1), policy.chooseVolume(volumes, 0));

    // The first volume has only 100L space, so the policy should
    // choose the second one in case we ask for more.
    Assert.assertEquals(volumes.get(1),
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
    final List<VolumeInfo> volumes = new ArrayList<>();

    // First volume, with 100 bytes of space.
    volumes.add(Mockito.mock(VolumeInfo.class));
    Mockito.when(volumes.get(0).getAvailable()).thenReturn(100L);

    // Second volume, with 200 bytes of space.
    volumes.add(Mockito.mock(VolumeInfo.class));
    Mockito.when(volumes.get(1).getAvailable()).thenReturn(200L);

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
}
