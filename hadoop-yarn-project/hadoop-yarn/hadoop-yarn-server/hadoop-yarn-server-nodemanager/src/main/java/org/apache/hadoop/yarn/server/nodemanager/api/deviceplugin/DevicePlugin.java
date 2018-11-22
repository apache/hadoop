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

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

import java.util.Set;

/**
 * A must interface for vendor plugin to implement.
 * */
public interface DevicePlugin {
  /**
   * Called first when device plugin framework wants to register.
   * @return DeviceRegisterRequest {@link DeviceRegisterRequest}
   * @throws Exception
   * */
  DeviceRegisterRequest getRegisterRequestInfo()
      throws Exception;

  /**
   * Called when update node resource.
   * @return a set of {@link Device}, {@link java.util.TreeSet} recommended
   * @throws Exception
   * */
  Set<Device> getDevices() throws Exception;

  /**
   * Asking how these devices should be prepared/used
   * before/when container launch. A plugin can do some tasks in its own or
   * define it in DeviceRuntimeSpec to let the framework do it.
   * For instance, define {@code VolumeSpec} to let the
   * framework to create volume before running container.
   *
   * @param allocatedDevices A set of allocated {@link Device}.
   * @param yarnRuntime Indicate which runtime YARN will use
   *        Could be {@code RUNTIME_DEFAULT} or {@code RUNTIME_DOCKER}
   *        in {@link DeviceRuntimeSpec} constants. The default means YARN's
   *        non-docker container runtime is used. The docker means YARN's
   *        docker container runtime is used.
   * @return a {@link DeviceRuntimeSpec} description about environment,
   * {@link         VolumeSpec}, {@link MountVolumeSpec}. etc
   * @throws Exception
   * */
  DeviceRuntimeSpec onDevicesAllocated(Set<Device> allocatedDevices,
      YarnRuntimeType yarnRuntime) throws Exception;

  /**
   * Called after device released.
   * @param releasedDevices A set of released devices
   * @throws Exception
   * */
  void onDevicesReleased(Set<Device> releasedDevices)
      throws Exception;
}
