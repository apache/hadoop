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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The response sent by the <code>NodeManager</code> to the 
 * <code>ApplicationMaster</code> when asked to <em>start</em> an
 * allocated container.</p>
 * 
 * @see ContainerManagementProtocol#startContainer(StartContainerRequest)
 */
@Public
@Stable
public abstract class StartContainerResponse {

  @Private
  @Unstable
  public static StartContainerResponse newInstance(
      Map<String, ByteBuffer> servicesMetaData) {
    StartContainerResponse response =
        Records.newRecord(StartContainerResponse.class);
    response.setAllServicesMetaData(servicesMetaData);
    return response;
  }

  /**
   * <p>
   * Get the meta-data from all auxiliary services running on the
   * <code>NodeManager</code>.
   * </p>
   * <p>
   * The meta-data is returned as a Map between the auxiliary service names and
   * their corresponding per service meta-data as an opaque blob
   * <code>ByteBuffer</code>
   * </p>
   * 
   * <p>
   * To be able to interpret the per-service meta-data, you should consult the
   * documentation for the Auxiliary-service configured on the NodeManager
   * </p>
   * 
   * @return a Map between the names of auxiliary services and their
   *         corresponding meta-data
   */
  @Public
  @Stable
  public abstract Map<String, ByteBuffer> getAllServicesMetaData();

  /**
   * Set to the list of auxiliary services which have been started on the
   * <code>NodeManager</code>. This is done only once when the
   * <code>NodeManager</code> starts up
   * @param allServicesMetaData A map from auxiliary service names to the opaque
   * blob <code>ByteBuffer</code> for that auxiliary service
   */
  @Private
  @Unstable
  public abstract void setAllServicesMetaData(
      Map<String, ByteBuffer> allServicesMetaData);
}
