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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningResults;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningTask;

import java.util.concurrent.ScheduledFuture;

/**
 * Main interface for volume manager that manages all volumes.
 * Volume manager talks to a CSI controller plugin to handle the
 * volume operations before it is available to be published on
 * any node manager.
 */
@Private
@Unstable
public interface VolumeManager {

  /**
   * @return all known volumes and their states.
   */
  VolumeStates getVolumeStates();

  /**
   * Start to supervise on a volume.
   * @param volume volume.
   * @return the volume being managed by the manager.
   */
  Volume addOrGetVolume(Volume volume);

  /**
   * Execute volume provisioning tasks as backend threads.
   * @param volumeProvisioningTask  A provisioning task encapsulates
   * all the logic required by a storage system to provision a volume.
   * @param delaySecond delay Second.
   * @return ScheduledFuture.
   */
  ScheduledFuture<VolumeProvisioningResults> schedule(
      VolumeProvisioningTask volumeProvisioningTask, int delaySecond);

  /**
   * Register a csi-driver-adaptor to the volume manager.
   * @param driverName driver name.
   * @param client csi adaptor protocol client.
   */
  void registerCsiDriverAdaptor(String driverName, CsiAdaptorProtocol client);

  /**
   * Returns the csi-driver-adaptor client from cache by the given driver name.
   * If the client is not found, null is returned.
   * @param driverName driver name.
   * @return a csi-driver-adaptor client working for given driver or null
   * if the adaptor could not be found.
   */
  CsiAdaptorProtocol getAdaptorByDriverName(String driverName);
}
