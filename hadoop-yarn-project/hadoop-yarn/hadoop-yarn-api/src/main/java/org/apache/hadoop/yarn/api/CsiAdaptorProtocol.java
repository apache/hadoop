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
package org.apache.hadoop.yarn.api;

import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * CSI adaptor delegates all the calls from YARN to a CSI driver.
 */
public interface CsiAdaptorProtocol {

  /**
   * Get plugin info from the CSI driver. The driver usually returns
   * the name of the driver and its version.
   * @param request get plugin info request.
   * @return response that contains driver name and its version.
   * @throws YarnException
   * @throws IOException
   */
  GetPluginInfoResponse getPluginInfo(GetPluginInfoRequest request)
      throws YarnException, IOException;

  /**
   * Validate if the volume capacity can be satisfied on the underneath
   * storage system. This method responses if the capacity can be satisfied
   * or not, with a detailed message.
   * @param request validate volume capability request.
   * @return validation response.
   * @throws YarnException
   * @throws IOException
   */
  ValidateVolumeCapabilitiesResponse validateVolumeCapacity(
      ValidateVolumeCapabilitiesRequest request) throws YarnException,
      IOException;

  /**
   * Publish the volume on a node manager, the volume will be mounted
   * to the local file system and become visible for clients.
   * @param request publish volume request.
   * @return publish volume response.
   * @throws YarnException
   * @throws IOException
   */
  NodePublishVolumeResponse nodePublishVolume(
      NodePublishVolumeRequest request) throws YarnException, IOException;

  /**
   * This is a reverse operation of
   * {@link #nodePublishVolume(NodePublishVolumeRequest)}, it un-mounts the
   * volume from given node.
   * @param request un-publish volume request.
   * @return un-publish volume response.
   * @throws YarnException
   * @throws IOException
   */
  NodeUnpublishVolumeResponse nodeUnpublishVolume(
      NodeUnpublishVolumeRequest request) throws YarnException, IOException;
}
