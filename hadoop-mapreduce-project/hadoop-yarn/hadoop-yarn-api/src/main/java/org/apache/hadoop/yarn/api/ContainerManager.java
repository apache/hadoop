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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

/**
 * <p>The protocol between an <code>ApplicationMaster</code> and a 
 * <code>NodeManager</code> to start/stop containers and to get status
 * of running containers.</p>
 * 
 * <p>If security is enabled the <code>NodeManager</code> verifies that the
 * <code>ApplicationMaster</code> has truly been allocated the container
 * by the <code>ResourceManager</code> and also verifies all interactions such 
 * as stopping the container or obtaining status information for the container.
 * </p>
 */
@Public
@Stable
public interface ContainerManager {
  /**
   * <p>The <code>ApplicationMaster</code> requests a <code>NodeManager</code>
   * to <em>start</em> a {@link Container} allocated to it using this interface.
   * </p>
   * 
   * <p>The <code>ApplicationMaster</code> has to provide details such as
   * allocated resource capability, security tokens (if enabled), command
   * to be executed to start the container, environment for the process, 
   * necessary binaries/jar/shared-objects etc. via the 
   * {@link ContainerLaunchContext} in the {@link StartContainerRequest}.</p>
   * 
   * <p>Currently the <code>NodeManager</code> sends an immediate, empty 
   * response via {@link StartContainerResponse} to signify acceptance of the
   * request and throws an exception in case of errors. The 
   * <code>ApplicationMaster</code> can use 
   * {@link #getContainerStatus(GetContainerStatusRequest)} to get updated 
   * status of the to-be-launched or launched container.</p>
   * 
   * @param request request to start a container
   * @return empty response to indicate acceptance of the request 
   *         or an exception
   * @throws YarnRemoteException
   */
  @Public
  @Stable
  StartContainerResponse startContainer(StartContainerRequest request)
      throws YarnRemoteException;

  /**
   * <p>The <code>ApplicationMaster</code> requests a <code>NodeManager</code>
   * to <em>stop</em> a {@link Container} allocated to it using this interface.
   * </p>
   * 
   * <p>The <code>ApplicationMaster</code></p> sends a 
   * {@link StopContainerRequest} which includes the {@link ContainerId} of the
   * container to be stopped.</p>
   * 
   * <p>Currently the <code>NodeManager</code> sends an immediate, empty 
   * response via {@link StopContainerResponse} to signify acceptance of the
   * request and throws an exception in case of errors. The 
   * <code>ApplicationMaster</code> can use 
   * {@link #getContainerStatus(GetContainerStatusRequest)} to get updated 
   * status of the container.</p>
   * 
   * @param request request to stop a container
   * @return empty response to indicate acceptance of the request 
   *         or an exception
   * @throws YarnRemoteException
   */
  @Public
  @Stable
  StopContainerResponse stopContainer(StopContainerRequest request)
      throws YarnRemoteException;

  /**
   * <p>The api used by the <code>ApplicationMaster</code> to request for 
   * current status of a <code>Container</code> from the 
   * <code>NodeManager</code>.</p>
   * 
   * <p>The <code>ApplicationMaster</code></p> sends a 
   * {@link GetContainerStatusRequest} which includes the {@link ContainerId} of 
   * the container whose status is needed.</p>
   *
   *<p>The <code>NodeManager</code> responds with 
   *{@link GetContainerStatusResponse} which includes the 
   *{@link ContainerStatus} of the container.</p>
   *
   * @param request request to get <code>ContainerStatus</code> of a container
   *                with the specified <code>ContainerId</code>
   * @return the <code>ContainerStatus</code> of the container
   * @throws YarnRemoteException
   */
  @Public
  @Stable
  GetContainerStatusResponse getContainerStatus(
      GetContainerStatusRequest request) throws YarnRemoteException;
}
