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
package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Map;

/**
 * YARN internal message used to validate volume capabilities
 * with a CSI driver controller plugin.
 */
public abstract class ValidateVolumeCapabilitiesRequest {

  /**
   * Volume access mode.
   */
  public enum AccessMode {
    UNKNOWN,
    SINGLE_NODE_WRITER,
    SINGLE_NODE_READER_ONLY,
    MULTI_NODE_READER_ONLY,
    MULTI_NODE_SINGLE_WRITER,
    MULTI_NODE_MULTI_WRITER,
  }

  /**
   * Volume type.
   */
  public enum VolumeType {
    BLOCK,
    FILE_SYSTEM
  }

  /**
   * Volume capability.
   */
  public static class VolumeCapability {

    private AccessMode mode;
    private VolumeType type;
    private List<String> flags;

    public VolumeCapability(AccessMode accessMode, VolumeType volumeType,
        List<String> mountFlags) {
      this.mode = accessMode;
      this.type = volumeType;
      this.flags = mountFlags;
    }

    public AccessMode getAccessMode() {
      return mode;
    }

    public VolumeType getVolumeType() {
      return type;
    }

    public List<String> getMountFlags() {
      return flags;
    }
  }

  public static ValidateVolumeCapabilitiesRequest newInstance(
      String volumeId, List<VolumeCapability> volumeCapabilities,
      Map<String, String> volumeAttributes) {
    ValidateVolumeCapabilitiesRequest
        request =
        Records.newRecord(
            ValidateVolumeCapabilitiesRequest.class);
    request.setVolumeId(volumeId);
    request.setVolumeAttributes(volumeAttributes);
    for (VolumeCapability capability : volumeCapabilities) {
      request.addVolumeCapability(capability);
    }
    return request;
  }

  public static ValidateVolumeCapabilitiesRequest newInstance(
      String volumeId, Map<String, String> volumeAttributes) {
    ValidateVolumeCapabilitiesRequest
        request =
        Records.newRecord(
            ValidateVolumeCapabilitiesRequest.class);
    request.setVolumeId(volumeId);
    request.setVolumeAttributes(volumeAttributes);
    return request;
  }

  public abstract void setVolumeId(String volumeId);

  public abstract String getVolumeId();

  public abstract void setVolumeAttributes(Map<String, String> attributes);

  public abstract Map<String, String> getVolumeAttributes();

  public abstract void addVolumeCapability(VolumeCapability volumeCapability);

  public abstract List<VolumeCapability> getVolumeCapabilities();
}
