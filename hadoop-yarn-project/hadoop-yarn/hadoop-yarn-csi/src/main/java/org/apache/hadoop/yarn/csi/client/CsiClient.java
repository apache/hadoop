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

package org.apache.hadoop.yarn.csi.client;

import csi.v0.Csi;
import csi.v0.Csi.GetPluginInfoResponse;

import java.io.IOException;

/**
 * General interface for a CSI client. This interface defines all APIs
 * that CSI spec supports, including both identity/controller/node service
 * APIs.
 */
public interface CsiClient {

  /**
   * Gets some basic info about the CSI plugin, including the driver name,
   * version and optionally some manifest info.
   * @return {@link GetPluginInfoResponse}
   * @throws IOException when unable to get plugin info from the driver.
   */
  GetPluginInfoResponse getPluginInfo() throws IOException;

  Csi.ValidateVolumeCapabilitiesResponse validateVolumeCapabilities(
      Csi.ValidateVolumeCapabilitiesRequest request) throws IOException;

  Csi.NodePublishVolumeResponse nodePublishVolume(
      Csi.NodePublishVolumeRequest request) throws IOException;

  Csi.NodeUnpublishVolumeResponse nodeUnpublishVolume(
      Csi.NodeUnpublishVolumeRequest request) throws IOException;
}
