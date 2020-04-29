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

import java.io.IOException;

/**
 * This interface is used only in testing. It gives default implementation
 * of all methods.
 */
public interface ICsiClientTest extends CsiClient {

  @Override
  default Csi.GetPluginInfoResponse getPluginInfo()
      throws IOException {
    return null;
  }

  @Override
  default Csi.ValidateVolumeCapabilitiesResponse validateVolumeCapabilities(
      Csi.ValidateVolumeCapabilitiesRequest request) throws IOException {
    return null;
  }

  @Override
  default Csi.NodePublishVolumeResponse nodePublishVolume(
      Csi.NodePublishVolumeRequest request) throws IOException {
    return null;
  }

  @Override
  default Csi.NodeUnpublishVolumeResponse nodeUnpublishVolume(
      Csi.NodeUnpublishVolumeRequest request) throws IOException {
    return null;
  }
}
