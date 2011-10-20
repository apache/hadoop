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

package org.apache.hadoop.yarn.api.records;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ContainerManager;

/**
 * <p><code>ContainerLaunchContext</code> represents the all of the information
 * needed by the <code>NodeManager</code> to launch a container.</p>
 * 
 * <p>It includes details such as:
 *   <ul>
 *     <li>{@link ContainerId} of the container.</li>
 *     <li>{@link Resource} allocated to the container.</li>
 *     <li>User to whom the container is allocated.</li>
 *     <li>Security tokens (if security is enabled).</li>
 *     <li>
 *       {@link LocalResource} necessary for running the container such
 *       as binaries, jar, shared-objects, side-files etc. 
 *     </li>
 *     <li>Optional, application-specific binary service data.</li>
 *     <li>Environment variables for the launched process.</li>
 *     <li>Command to launch the container.</li>
 *   </ul>
 * </p>
 * 
 * @see ContainerManager#startContainer(org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest)
 */
@Public
@Stable
public interface ContainerLaunchContext {
  /**
   * Get <code>ContainerId</code> of container to be launched.
   * @return <code>ContainerId</code> of container to be launched
   */
  @Public
  @Stable
  ContainerId getContainerId();

  /**
   * Set <code>ContainerId</code> of container to be launched.
   * @param containerId et <code>ContainerId</code> of container to be launched
   */
  @Public
  @Stable
  void setContainerId(ContainerId containerId);

  /**
   * Get the <em>user</em> to whom the container has been allocated.
   * @return the <em>user</em> to whom the container has been allocated
   */
  @Public
  @Stable
  String getUser();
  
  /**
   * Set the <em>user</em> to whom the container has been allocated
   * @param user <em>user</em> to whom the container has been allocated
   */
  @Public
  @Stable
  void setUser(String user);

  /**
   * Get the <code>Resource</code> allocated to the container by the
   * <code>ResourceManager</code>.
   * @return <code>Resource</code> allocated to the container by the
   *         <code>ResourceManager</code>
   */
  @Public
  @Stable
  Resource getResource();

  /**
   * Set the <code>Resource</code> allocated to the container by the
   * <code>ResourceManager</code>.
   * @param resource allocated resource
   */
  @Public
  @Stable
  void setResource(Resource resource);

  /**
   * Get security tokens (if security is enabled).
   * @return security tokens (if security is enabled)
   */
  @Public
  @Stable
  ByteBuffer getContainerTokens();

  /**
   * Set security tokens (if security is enabled).
   * @param containerToken security tokens 
   */
  @Public
  @Stable
  void setContainerTokens(ByteBuffer containerToken);

  /**
   * Get <code>LocalResource</code> required by the container.
   * @return all <code>LocalResource</code> required by the container
   */
  @Public
  @Stable
  Map<String, LocalResource> getLocalResources();
  
  /**
   * Set <code>LocalResource</code> required by the container.
   * @param localResources <code>LocalResource</code> required by the container
   */
  @Public
  @Stable
  void setLocalResources(Map<String, LocalResource> localResources);

  /**
   * Get application-specific binary <em>service data</em>.
   * @return application-specific binary <em>service data</em>
   */
  @Public
  @Stable
  Map<String, ByteBuffer> getServiceData();
  
  /**
   * Set application-specific binary <em>service data</em>.
   * @param serviceData application-specific binary <em>service data</em>
   */
  @Public
  @Stable
  void setServiceData(Map<String, ByteBuffer> serviceData);

  /**
   * Get <em>environment variables</em> for the container.
   * @return <em>environment variables</em> for the container
   */
  @Public
  @Stable
  Map<String, String> getEnvironment();
    
  /**
   * Add <em>environment variables</em> for the container.
   * @param environment <em>environment variables</em> for the container
   */
  @Public
  @Stable
  void setEnvironment(Map<String, String> environment);

  /**
   * Get the list of <em>commands</em> for launching the container.
   * @return the list of <em>commands</em> for launching the container
   */
  @Public
  @Stable
  List<String> getCommands();
  
  /**
   * Add the list of <em>commands</em> for launching the container.
   * @param commands the list of <em>commands</em> for launching the container
   */
  @Public
  @Stable
  void setCommands(List<String> commands);

  /**
   * Get the <code>ApplicationACL</code>s for the application. 
   * @return all the <code>ApplicationACL</code>s
   */
  @Public
  @Stable
  public Map<ApplicationAccessType, String> getApplicationACLs();

  /**
   * Set the <code>ApplicationACL</code>s for the application. 
   * @param acls
   */
  @Public
  @Stable
  public void setApplicationACLs(Map<ApplicationAccessType, String> acls);
}
