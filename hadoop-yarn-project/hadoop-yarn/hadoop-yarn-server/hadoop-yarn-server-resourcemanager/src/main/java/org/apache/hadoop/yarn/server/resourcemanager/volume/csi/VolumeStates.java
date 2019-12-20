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
import org.apache.hadoop.yarn.server.volume.csi.VolumeId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Volume manager states, including all managed volumes and their states.
 */
public class VolumeStates {

  private final Map<VolumeId, Volume> volumeStates;

  public VolumeStates() {
    this.volumeStates = new ConcurrentHashMap<>();
  }

  public Volume getVolume(VolumeId volumeId) {
    return volumeStates.get(volumeId);
  }

  /**
   * Add volume if it is not yet added.
   * If a new volume is added with a same {@link VolumeId}
   * with a existing volume, existing volume will be returned.
   * @param volume volume to add
   * @return volume added or existing volume
   */
  public Volume addVolumeIfAbsent(Volume volume) {
    if (volume.getVolumeId() != null) {
      return volumeStates.putIfAbsent(volume.getVolumeId(), volume);
    } else {
      // for dynamical provisioned volumes,
      // the volume ID might not be available at time being.
      // we can makeup one with the combination of driver+volumeName+timestamp
      // once the volume ID is generated, we should replace ID.
      return volume;
    }
  }
}
