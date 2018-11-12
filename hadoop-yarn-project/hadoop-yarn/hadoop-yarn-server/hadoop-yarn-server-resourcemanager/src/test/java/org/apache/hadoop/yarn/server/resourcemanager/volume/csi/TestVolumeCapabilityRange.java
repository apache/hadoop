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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi;

import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;
import org.apache.hadoop.yarn.server.volume.csi.VolumeCapabilityRange;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for volume capability.
 */
public class TestVolumeCapabilityRange {

  @Test(expected = InvalidVolumeException.class)
  public void testInvalidMinCapability() throws InvalidVolumeException {
    VolumeCapabilityRange.newBuilder()
        .minCapacity(-1L)
        .maxCapacity(5L)
        .unit("Gi")
        .build();
  }

  @Test(expected = InvalidVolumeException.class)
  public void testMissingMinCapability() throws InvalidVolumeException {
    VolumeCapabilityRange.newBuilder()
        .maxCapacity(5L)
        .unit("Gi")
        .build();
  }

  @Test(expected = InvalidVolumeException.class)
  public void testMissingUnit() throws InvalidVolumeException {
    VolumeCapabilityRange.newBuilder()
        .minCapacity(0L)
        .maxCapacity(5L)
        .build();
  }

  @Test
  public void testGetVolumeCapability() throws InvalidVolumeException {
    VolumeCapabilityRange vc = VolumeCapabilityRange.newBuilder()
        .minCapacity(0L)
        .maxCapacity(5L)
        .unit("Gi")
        .build();

    Assert.assertEquals(0L, vc.getMinCapacity());
    Assert.assertEquals(5L, vc.getMaxCapacity());
    Assert.assertEquals("Gi", vc.getUnit());
  }
}
