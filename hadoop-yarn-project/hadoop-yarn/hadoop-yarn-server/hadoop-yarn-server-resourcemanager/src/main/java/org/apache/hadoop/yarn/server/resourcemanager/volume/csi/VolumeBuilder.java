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

import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.VolumeImpl;
import org.apache.hadoop.yarn.server.volume.csi.VolumeCapabilityRange;
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;
import org.apache.hadoop.yarn.server.volume.csi.VolumeMetaData;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;

import java.util.Optional;
import java.util.UUID;

/**
 * Helper class to build a {@link Volume}.
 */
public final class VolumeBuilder {

  private String id;
  private String name;
  private Long min;
  private Long max;
  private String unit;
  private String driver;
  private String mount;

  private VolumeBuilder() {
    // hide constructor
  }

  public static VolumeBuilder newBuilder() {
    return new VolumeBuilder();
  }

  public VolumeBuilder volumeId(String volumeId) {
    this.id = volumeId;
    return this;
  }

  public VolumeBuilder volumeName(String volumeName) {
    this.name = volumeName;
    return this;
  }

  public VolumeBuilder minCapability(long minCapability) {
    this.min = Long.valueOf(minCapability);
    return this;
  }

  public VolumeBuilder maxCapability(long maxCapability) {
    this.max = Long.valueOf(maxCapability);
    return this;
  }

  public VolumeBuilder unit(String capUnit) {
    this.unit = capUnit;
    return this;
  }

  public VolumeBuilder driverName(String driverName) {
    this.driver = driverName;
    return this;
  }

  public VolumeBuilder mountPoint(String mountPoint) {
    this.mount = mountPoint;
    return this;
  }

  public Volume build() throws InvalidVolumeException {
    VolumeId vid = new VolumeId(
        Optional.ofNullable(id)
            .orElse(UUID.randomUUID().toString()));

    VolumeCapabilityRange volumeCap = VolumeCapabilityRange.newBuilder()
        .minCapacity(Optional.ofNullable(min).orElse(0L))
        .maxCapacity(Optional.ofNullable(max).orElse(Long.MAX_VALUE))
        .unit(Optional.ofNullable(unit).orElse("Gi"))
        .build();

    VolumeMetaData meta = VolumeMetaData.newBuilder()
        .capability(volumeCap)
        .driverName(Optional.ofNullable(driver).orElse("test-driver"))
        .mountPoint(Optional.ofNullable(mount).orElse("/mnt/data"))
        .volumeName(name)
        .volumeId(vid)
        .build();
    return new VolumeImpl(meta);
  }
}
