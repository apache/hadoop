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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

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
 *     <li>Command to be executed to launch the container.</li>
 *     <li></li>
 *     <li></li>
 *     <li></li>
 *     <li></li>
 *     <li></li>
 *     <li></li>
 *     <li></li>
 *     <li></li>
 *   </ul>
 * </p>
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

  @Private
  @Unstable
  void setContainerId(ContainerId containerId);

  /**
   * Get the <em>user</em> to whom the container has been allocated.
   * @return the <em>user</em> to whom the container has been allocated
   */
  @Public
  @Stable
  String getUser();
  
  @Private
  @Unstable
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
  
  @Private
  @Unstable
  void setResource(Resource resource);

  /**
   * Get security tokens (if security is enabled).
   * @return security tokens (if security is enabled)
   */
  @Public
  @Stable
  ByteBuffer getContainerTokens();

  @Private
  @Unstable
  void setContainerTokens(ByteBuffer containerToken);

  /**
   * Get all <code>LocalResource</code> required by the container.
   * @return all <code>LocalResource</code> required by the container
   */
  @Public
  @Stable
  Map<String, LocalResource> getAllLocalResources();
  
  @Private
  @Unstable
  LocalResource getLocalResource(String key);
  
  @Private
  @Unstable
  void addAllLocalResources(Map<String, LocalResource> localResources);

  @Private
  @Unstable
  void setLocalResource(String key, LocalResource value);

  @Private
  @Unstable
  void removeLocalResource(String key);

  @Private
  @Unstable
  void clearLocalResources();

  /**
   * Get application-specific binary service data.
   * @return application-specific binary service data
   */
  @Public
  @Stable
  Map<String, ByteBuffer> getAllServiceData();
  
  @Private
  @Unstable
  ByteBuffer getServiceData(String key);
  
  @Private
  @Unstable
  void addAllServiceData(Map<String, ByteBuffer> serviceData);

  @Private
  @Unstable
  void setServiceData(String key, ByteBuffer value);

  @Private
  @Unstable
  void removeServiceData(String key);

  @Private
  @Unstable
  void clearServiceData();

  /**
   * Get <em>environment variables</em> for the launched container.
   * @return <em>environment variables</em> for the launched container
   */
  @Public
  @Stable
  Map<String, String> getAllEnv();
  
  @Private
  @Unstable
  String getEnv(String key);
  
  @Private
  @Unstable
  void addAllEnv(Map<String, String> env);

  @Private
  @Unstable
  void setEnv(String key, String value);

  @Private
  @Unstable
  void removeEnv(String key);

  @Private
  @Unstable
  void clearEnv();

  /**
   * Get the list of <em>commands</em> for launching the container.
   * @return the list of <em>commands</em> for launching the container
   */
  @Public
  @Stable
  List<String> getCommandList();
  
  @Private
  @Unstable
  String getCommand(int index);
  
  @Private
  @Unstable
  int getCommandCount();
  
  @Private
  @Unstable
  void addAllCommands(List<String> commands);
  
  @Private
  @Unstable
  void addCommand(String command);
  
  @Private
  @Unstable
  void removeCommand(int index);
  
  @Private
  @Unstable
  void clearCommands();
}
