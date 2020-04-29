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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.yarn.server.volume.csi.CsiConstants;
import org.apache.hadoop.yarn.server.volume.csi.VolumeCapabilityRange;
import org.apache.hadoop.yarn.server.volume.csi.VolumeMetaData;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * Test cases for volume specification definition and parsing.
 */
public class TestVolumeMetaData {

  @Test
  public void testPreprovisionedVolume() throws InvalidVolumeException {
    VolumeCapabilityRange cap = VolumeCapabilityRange.newBuilder()
        .minCapacity(1L)
        .maxCapacity(5L)
        .unit("Gi")
        .build();

    // When volume id is given, volume name is optional
    VolumeMetaData meta = VolumeMetaData.newBuilder()
        .volumeId(new VolumeId("id-000001"))
        .capability(cap)
        .driverName("csi-demo-driver")
        .mountPoint("/mnt/data")
        .build();

    Assert.assertEquals(new VolumeId("id-000001"), meta.getVolumeId());
    Assert.assertEquals(1L, meta.getVolumeCapabilityRange().getMinCapacity());
    Assert.assertEquals(5L, meta.getVolumeCapabilityRange().getMaxCapacity());
    Assert.assertEquals("Gi", meta.getVolumeCapabilityRange().getUnit());
    Assert.assertEquals("csi-demo-driver", meta.getDriverName());
    Assert.assertEquals("/mnt/data", meta.getMountPoint());
    Assert.assertNull(meta.getVolumeName());
    Assert.assertTrue(meta.isProvisionedVolume());

    // Test toString
    JsonParser parser = new JsonParser();
    JsonElement element = parser.parse(meta.toString());
    JsonObject json = element.getAsJsonObject();
    Assert.assertNotNull(json);
    Assert.assertNull(json.get(CsiConstants.CSI_VOLUME_NAME));
    Assert.assertEquals("id-000001",
        json.get(CsiConstants.CSI_VOLUME_ID).getAsString());
    Assert.assertEquals("csi-demo-driver",
        json.get(CsiConstants.CSI_DRIVER_NAME).getAsString());
    Assert.assertEquals("/mnt/data",
        json.get(CsiConstants.CSI_VOLUME_MOUNT).getAsString());

  }

  @Test
  public void testDynamicalProvisionedVolume() throws InvalidVolumeException {
    VolumeCapabilityRange cap = VolumeCapabilityRange.newBuilder()
        .minCapacity(1L)
        .maxCapacity(5L)
        .unit("Gi")
        .build();

    // When volume name is given, volume id is optional
    VolumeMetaData meta = VolumeMetaData.newBuilder()
        .volumeName("volume-name")
        .capability(cap)
        .driverName("csi-demo-driver")
        .mountPoint("/mnt/data")
        .build();
    Assert.assertNotNull(meta);

    Assert.assertEquals("volume-name", meta.getVolumeName());
    Assert.assertEquals(1L, meta.getVolumeCapabilityRange().getMinCapacity());
    Assert.assertEquals(5L, meta.getVolumeCapabilityRange().getMaxCapacity());
    Assert.assertEquals("Gi", meta.getVolumeCapabilityRange().getUnit());
    Assert.assertEquals("csi-demo-driver", meta.getDriverName());
    Assert.assertEquals("/mnt/data", meta.getMountPoint());
    Assert.assertFalse(meta.isProvisionedVolume());

    // Test toString
    JsonParser parser = new JsonParser();
    JsonElement element = parser.parse(meta.toString());
    JsonObject json = element.getAsJsonObject();
    Assert.assertNotNull(json);
    Assert.assertNull(json.get(CsiConstants.CSI_VOLUME_ID));
    Assert.assertEquals("volume-name",
        json.get(CsiConstants.CSI_VOLUME_NAME).getAsString());
    Assert.assertEquals("csi-demo-driver",
        json.get(CsiConstants.CSI_DRIVER_NAME).getAsString());
    Assert.assertEquals("/mnt/data",
        json.get(CsiConstants.CSI_VOLUME_MOUNT).getAsString());
  }

  @Test(expected = InvalidVolumeException.class)
  public void testMissingMountpoint() throws InvalidVolumeException {
    VolumeCapabilityRange cap = VolumeCapabilityRange.newBuilder()
        .minCapacity(1L)
        .maxCapacity(5L)
        .unit("Gi")
        .build();

    VolumeMetaData.newBuilder()
        .volumeId(new VolumeId("id-000001"))
        .capability(cap)
        .driverName("csi-demo-driver")
        .build();
  }


  @Test(expected = InvalidVolumeException.class)
  public void testMissingCsiDriverName() throws InvalidVolumeException {
    VolumeCapabilityRange cap = VolumeCapabilityRange.newBuilder()
        .minCapacity(1L)
        .maxCapacity(5L)
        .unit("Gi")
        .build();

    VolumeMetaData.newBuilder()
        .volumeId(new VolumeId("id-000001"))
        .capability(cap)
        .mountPoint("/mnt/data")
        .build();
  }

  @Test(expected = InvalidVolumeException.class)
  public void testMissingVolumeCapability() throws InvalidVolumeException {
    VolumeMetaData.newBuilder()
        .volumeId(new VolumeId("id-000001"))
        .driverName("csi-demo-driver")
        .mountPoint("/mnt/data")
        .build();
  }

  @Test
  public void testVolumeId() {
    VolumeId id1 = new VolumeId("test00001");
    VolumeId id11 = new VolumeId("test00001");
    VolumeId id2 = new VolumeId("test00002");

    Assert.assertEquals(id1, id11);
    Assert.assertEquals(id1.hashCode(), id11.hashCode());
    Assert.assertNotEquals(id1, id2);

    HashMap<VolumeId, String> map = new HashMap<>();
    map.put(id1, "1");
    Assert.assertEquals(1, map.size());
    Assert.assertEquals("1", map.get(id11));
    map.put(id11, "2");
    Assert.assertEquals(1, map.size());
    Assert.assertEquals("2", map.get(id11));
    Assert.assertEquals("2", map.get(new VolumeId("test00001")));

    Assert.assertNotEquals(id1, id2);
  }
}
