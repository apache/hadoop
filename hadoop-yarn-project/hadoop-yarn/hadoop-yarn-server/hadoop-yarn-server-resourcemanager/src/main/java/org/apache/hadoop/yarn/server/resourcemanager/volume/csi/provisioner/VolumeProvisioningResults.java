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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner;

import com.google.gson.JsonObject;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.VolumeState;
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;

import java.util.HashMap;
import java.util.Map;

/**
 * Result of volumes' provisioning.
 */
public class VolumeProvisioningResults {

  private Map<VolumeId, VolumeProvisioningResult> resultMap;

  public VolumeProvisioningResults() {
    this.resultMap = new HashMap<>();
  }

  public boolean isSuccess() {
    return !resultMap.isEmpty() && resultMap.values().stream()
        .allMatch(subResult -> subResult.isSuccess());
  }

  public String getBriefMessage() {
    JsonObject obj = new JsonObject();
    obj.addProperty("TotalVolumes", resultMap.size());

    JsonObject failed = new JsonObject();
    for (VolumeProvisioningResult result : resultMap.values()) {
      if (!result.isSuccess()) {
        failed.addProperty(result.getVolumeId().toString(),
            result.getVolumeState().name());
      }
    }
    obj.add("failedVolumesStates", failed);
    return obj.toString();
  }

  static class VolumeProvisioningResult {

    private VolumeId volumeId;
    private VolumeState volumeState;
    private boolean success;

    VolumeProvisioningResult(VolumeId volumeId, VolumeState state) {
      this.volumeId = volumeId;
      this.volumeState = state;
      this.success = state == VolumeState.NODE_READY;
    }

    public boolean isSuccess() {
      return this.success;
    }

    public VolumeId getVolumeId() {
      return this.volumeId;
    }

    public VolumeState getVolumeState() {
      return this.volumeState;
    }
  }

  public void addResult(VolumeId volumeId, VolumeState state) {
    this.resultMap.put(volumeId,
        new VolumeProvisioningResult(volumeId, state));
  }
}
