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
package org.apache.hadoop.yarn.server.volume.csi;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import com.google.gson.JsonObject;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;

import java.util.ArrayList;
import java.util.List;

/**
 * VolumeMetaData defines all valid info for a CSI compatible volume.
 */
public class VolumeMetaData {

  private VolumeId volumeId;
  private String volumeName;
  private VolumeCapabilityRange volumeCapabilityRange;
  private String driverName;
  private String mountPoint;

  private void setVolumeId(VolumeId volumeId) {
    this.volumeId = volumeId;
  }

  private void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  private void setVolumeCapabilityRange(VolumeCapabilityRange capability) {
    this.volumeCapabilityRange = capability;
  }

  private void setDriverName(String driverName) {
    this.driverName = driverName;
  }

  private void setMountPoint(String mountPoint) {
    this.mountPoint = mountPoint;
  }

  public boolean isProvisionedVolume() {
    return this.volumeId != null;
  }

  public VolumeId getVolumeId() {
    return volumeId;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public VolumeCapabilityRange getVolumeCapabilityRange() {
    return volumeCapabilityRange;
  }

  public String getDriverName() {
    return driverName;
  }

  public String getMountPoint() {
    return mountPoint;
  }

  public static VolumeSpecBuilder newBuilder() {
    return new VolumeSpecBuilder();
  }

  public static List<VolumeMetaData> fromResource(
      ResourceInformation resourceInfo) throws InvalidVolumeException {
    List<VolumeMetaData> volumeMetaData = new ArrayList<>();
    if (resourceInfo != null) {
      if (resourceInfo.getTags() != null && resourceInfo.getTags()
          .contains(CsiConstants.CSI_VOLUME_RESOURCE_TAG)) {
        VolumeSpecBuilder builder = VolumeMetaData.newBuilder();
        // Volume ID
        if (resourceInfo.getAttributes()
            .containsKey(CsiConstants.CSI_VOLUME_ID)) {
          String id = resourceInfo.getAttributes()
              .get(CsiConstants.CSI_VOLUME_ID);
          builder.volumeId(new VolumeId(id));
        }
        // Volume name
        if (resourceInfo.getAttributes()
            .containsKey(CsiConstants.CSI_VOLUME_NAME)) {
          builder.volumeName(resourceInfo.getAttributes()
              .get(CsiConstants.CSI_VOLUME_NAME));
        }
        // CSI driver name
        if (resourceInfo.getAttributes()
            .containsKey(CsiConstants.CSI_DRIVER_NAME)) {
          builder.driverName(resourceInfo.getAttributes()
              .get(CsiConstants.CSI_DRIVER_NAME));
        }
        // Mount path
        if (resourceInfo.getAttributes()
            .containsKey(CsiConstants.CSI_VOLUME_MOUNT)) {
          builder.mountPoint(resourceInfo.getAttributes()
              .get(CsiConstants.CSI_VOLUME_MOUNT));
        }
        // Volume capability
        VolumeCapabilityRange volumeCapabilityRange =
            VolumeCapabilityRange.newBuilder()
                .minCapacity(resourceInfo.getValue())
                .unit(resourceInfo.getUnits())
                .build();
        builder.capability(volumeCapabilityRange);
        volumeMetaData.add(builder.build());
      }
    }
    return volumeMetaData;
  }

  @Override
  public String toString() {
    JsonObject json = new JsonObject();
    if (!Strings.isNullOrEmpty(volumeName)) {
      json.addProperty(CsiConstants.CSI_VOLUME_NAME, volumeName);
    }
    if (volumeId != null) {
      json.addProperty(CsiConstants.CSI_VOLUME_ID, volumeId.toString());
    }
    if (volumeCapabilityRange != null) {
      json.addProperty(CsiConstants.CSI_VOLUME_CAPABILITY,
          volumeCapabilityRange.toString());
    }
    if (!Strings.isNullOrEmpty(driverName)) {
      json.addProperty(CsiConstants.CSI_DRIVER_NAME, driverName);
    }
    if (!Strings.isNullOrEmpty(mountPoint)) {
      json.addProperty(CsiConstants.CSI_VOLUME_MOUNT, mountPoint);
    }
    return json.toString();
  }

  /**
   * The builder used to build a VolumeMetaData instance.
   */
  public static class VolumeSpecBuilder {
    // @CreateVolumeRequest
    // The suggested name for the storage space.
    private VolumeId volumeId;
    private String volumeName;
    private VolumeCapabilityRange volumeCapabilityRange;
    private String driverName;
    private String mountPoint;

    public VolumeSpecBuilder volumeId(VolumeId volumeId) {
      this.volumeId = volumeId;
      return this;
    }

    public VolumeSpecBuilder volumeName(String name) {
      this.volumeName = name;
      return this;
    }

    public VolumeSpecBuilder driverName(String driverName) {
      this.driverName = driverName;
      return this;
    }

    public VolumeSpecBuilder mountPoint(String mountPoint) {
      this.mountPoint = mountPoint;
      return this;
    }

    public VolumeSpecBuilder capability(VolumeCapabilityRange capability) {
      this.volumeCapabilityRange = capability;
      return this;
    }

    public VolumeMetaData build() throws InvalidVolumeException {
      VolumeMetaData spec = new VolumeMetaData();
      spec.setVolumeId(volumeId);
      spec.setVolumeName(volumeName);
      spec.setVolumeCapabilityRange(volumeCapabilityRange);
      spec.setDriverName(driverName);
      spec.setMountPoint(mountPoint);
      validate(spec);
      return spec;
    }

    private void validate(VolumeMetaData spec) throws InvalidVolumeException {
      // Volume name OR Volume ID must be set
      if (Strings.isNullOrEmpty(spec.getVolumeName())
          && spec.getVolumeId() == null) {
        throw new InvalidVolumeException("Invalid volume, both volume name"
            + " and ID are missing from the spec. Volume spec: "
            + spec.toString());
      }
      // Volume capability must be set
      if (spec.getVolumeCapabilityRange() == null) {
        throw new InvalidVolumeException("Invalid volume, volume capability"
            + " is missing. Volume spec: " + spec.toString());
      }
      // CSI driver name must be set
      if (Strings.isNullOrEmpty(spec.getDriverName())) {
        throw new InvalidVolumeException("Invalid volume, the csi-driver name"
            + " is missing. Volume spec: " + spec.toString());
      }
      // Mount point must be set
      if (Strings.isNullOrEmpty(spec.getMountPoint())) {
        throw new InvalidVolumeException("Invalid volume, the mount point"
            + " is missing. Volume spec: " + spec.toString());
      }
    }
  }
}
