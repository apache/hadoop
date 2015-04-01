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
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code ContainerLaunchContext} represents all of the information
 * needed by the {@code NodeManager} to launch a container.
 * <p>
 * It includes details such as:
 * <ul>
 *   <li>{@link ContainerId} of the container.</li>
 *   <li>{@link Resource} allocated to the container.</li>
 *   <li>User to whom the container is allocated.</li>
 *   <li>Security tokens (if security is enabled).</li>
 *   <li>
 *     {@link LocalResource} necessary for running the container such
 *     as binaries, jar, shared-objects, side-files etc.
 *   </li>
 *   <li>Optional, application-specific binary service data.</li>
 *   <li>Environment variables for the launched process.</li>
 *   <li>Command to launch the container.</li>
 * </ul>
 * 
 * @see ContainerManagementProtocol#startContainers(org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest)
 */
@Public
@Stable
public abstract class ContainerLaunchContext {

  @Public
  @Stable
  public static ContainerLaunchContext newInstance(
      Map<String, LocalResource> localResources,
      Map<String, String> environment, List<String> commands,
      Map<String, ByteBuffer> serviceData,  ByteBuffer tokens,
      Map<ApplicationAccessType, String> acls) {
    ContainerLaunchContext container =
        Records.newRecord(ContainerLaunchContext.class);
    container.setLocalResources(localResources);
    container.setEnvironment(environment);
    container.setCommands(commands);
    container.setServiceData(serviceData);
    container.setTokens(tokens);
    container.setApplicationACLs(acls);
    return container;
  }

  /**
   * Get all the tokens needed by this container. It may include file-system
   * tokens, ApplicationMaster related tokens if this container is an
   * ApplicationMaster or framework level tokens needed by this container to
   * communicate to various services in a secure manner.
   * 
   * @return tokens needed by this container.
   */
  @Public
  @Stable
  public abstract ByteBuffer getTokens();

  /**
   * Set security tokens needed by this container.
   * @param tokens security tokens 
   */
  @Public
  @Stable
  public abstract void setTokens(ByteBuffer tokens);

  /**
   * Get <code>LocalResource</code> required by the container.
   * @return all <code>LocalResource</code> required by the container
   */
  @Public
  @Stable
  public abstract Map<String, LocalResource> getLocalResources();
  
  /**
   * Set <code>LocalResource</code> required by the container. All pre-existing
   * Map entries are cleared before adding the new Map
   * @param localResources <code>LocalResource</code> required by the container
   */
  @Public
  @Stable
  public abstract void setLocalResources(Map<String, LocalResource> localResources);

  /**
   * <p>
   * Get application-specific binary <em>service data</em>. This is a map keyed
   * by the name of each {@link AuxiliaryService} that is configured on a
   * NodeManager and value correspond to the application specific data targeted
   * for the keyed {@link AuxiliaryService}.
   * </p>
   * 
   * <p>
   * This will be used to initialize this application on the specific
   * {@link AuxiliaryService} running on the NodeManager by calling
   * {@link AuxiliaryService#initializeApplication(ApplicationInitializationContext)}
   * </p>
   * 
   * @return application-specific binary <em>service data</em>
   */
  @Public
  @Stable
  public abstract Map<String, ByteBuffer> getServiceData();
  
  /**
   * <p>
   * Set application-specific binary <em>service data</em>. This is a map keyed
   * by the name of each {@link AuxiliaryService} that is configured on a
   * NodeManager and value correspond to the application specific data targeted
   * for the keyed {@link AuxiliaryService}. All pre-existing Map entries are
   * preserved.
   * </p>
   * 
   * @param serviceData
   *          application-specific binary <em>service data</em>
   */
  @Public
  @Stable
  public abstract void setServiceData(Map<String, ByteBuffer> serviceData);

  /**
   * Get <em>environment variables</em> for the container.
   * @return <em>environment variables</em> for the container
   */
  @Public
  @Stable
  public abstract Map<String, String> getEnvironment();
    
  /**
   * Add <em>environment variables</em> for the container. All pre-existing Map
   * entries are cleared before adding the new Map
   * @param environment <em>environment variables</em> for the container
   */
  @Public
  @Stable
  public abstract void setEnvironment(Map<String, String> environment);

  /**
   * Get the list of <em>commands</em> for launching the container.
   * @return the list of <em>commands</em> for launching the container
   */
  @Public
  @Stable
  public abstract List<String> getCommands();
  
  /**
   * Add the list of <em>commands</em> for launching the container. All
   * pre-existing List entries are cleared before adding the new List
   * @param commands the list of <em>commands</em> for launching the container
   */
  @Public
  @Stable
  public abstract void setCommands(List<String> commands);

  /**
   * Get the <code>ApplicationACL</code>s for the application. 
   * @return all the <code>ApplicationACL</code>s
   */
  @Public
  @Stable
  public abstract  Map<ApplicationAccessType, String> getApplicationACLs();

  /**
   * Set the <code>ApplicationACL</code>s for the application. All pre-existing
   * Map entries are cleared before adding the new Map
   * @param acls <code>ApplicationACL</code>s for the application
   */
  @Public
  @Stable
  public abstract  void setApplicationACLs(Map<ApplicationAccessType, String> acls);
}
