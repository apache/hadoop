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

import java.util.Map;
import java.util.Set;

/**
 * An optional interface to implement if custom device scheduling is needed.
 * If this is not implemented, the device framework will do scheduling.
 * */
public interface DevicePluginScheduler {
  /**
   * Called when allocating devices. The framework will do all device book
   * keeping and fail recovery. So this hook could be stateless and only do
   * scheduling based on available devices passed in. It could be
   * invoked multiple times by the framework. The hint in environment variables
   * passed in could be potentially used in making better scheduling decision.
   * For instance, GPU scheduling might support different kind of policy. The
   * container can set it through environment variables.
   * @param availableDevices Devices allowed to be chosen from.
   * @param count Number of device to be allocated.
   * @param env Environment variables of the container.
   * @return A set of {@link Device} allocated
   * */
  Set<Device> allocateDevices(Set<Device> availableDevices, int count,
      Map<String, String> env);
}
