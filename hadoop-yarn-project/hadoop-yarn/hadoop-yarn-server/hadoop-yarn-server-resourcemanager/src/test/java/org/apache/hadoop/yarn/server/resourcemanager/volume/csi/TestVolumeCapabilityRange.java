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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assertions;

/**
 * Test cases for volume capability.
 */
public class TestVolumeCapabilityRange {

  @Test
  void testInvalidMinCapability() throws InvalidVolumeException {
    Assertions.assertThrows(InvalidVolumeException.class, () -> {
      VolumeCapabilityRange.newBuilder()
          .minCapacity(-1L)
          .maxCapacity(5L)
          .unit("Gi")
          .build();
    });
  }

  @Test
  void testMissingMinCapability() throws InvalidVolumeException {
    Assertions.assertThrows(InvalidVolumeException.class, () -> {
      VolumeCapabilityRange.newBuilder()
          .maxCapacity(5L)
          .unit("Gi")
          .build();
    });
  }

  @Test
  void testMissingUnit() throws InvalidVolumeException {
    Assertions.assertThrows(InvalidVolumeException.class, () -> {
      VolumeCapabilityRange.newBuilder()
          .minCapacity(0L)
          .maxCapacity(5L)
          .build();
    });
  }

  @Test
  void testGetVolumeCapability() throws InvalidVolumeException {
    VolumeCapabilityRange vc = VolumeCapabilityRange.newBuilder()
        .minCapacity(0L)
        .maxCapacity(5L)
        .unit("Gi")
        .build();

    Assertions.assertEquals(0L, vc.getMinCapacity());
    Assertions.assertEquals(5L, vc.getMaxCapacity());
    Assertions.assertEquals("Gi", vc.getUnit());
  }
}
