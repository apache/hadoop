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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code Container} represents an allocated resource in the cluster.
 * <p>
 * The {@code ResourceManager} is the sole authority to allocate any
 * {@code Container} to applications. The allocated {@code Container}
 * is always on a single node and has a unique {@link ContainerId}. It has
 * a specific amount of {@link Resource} allocated.
 * <p>
 * It includes details such as:
 * <ul>
 *   <li>{@link ContainerId} for the container, which is globally unique.</li>
 *   <li>
 *     {@link NodeId} of the node on which it is allocated.
 *   </li>
 *   <li>HTTP uri of the node.</li>
 *   <li>{@link Resource} allocated to the container.</li>
 *   <li>{@link Priority} at which the container was allocated.</li>
 *   <li>
 *     Container {@link Token} of the container, used to securely verify
 *     authenticity of the allocation.
 *   </li>
 * </ul>
 * 
 * Typically, an {@code ApplicationMaster} receives the {@code Container}
 * from the {@code ResourceManager} during resource-negotiation and then
 * talks to the {@code NodeManager} to start/stop containers.
 * 
 * @see ApplicationMasterProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
 * @see ContainerManagementProtocol#startContainers(org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest)
 * @see ContainerManagementProtocol#stopContainers(org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest)
 */
@Public
@Stable
public abstract class Container implements Comparable<Container> {

  @Private
  @Unstable
  public static Container newInstance(ContainerId containerId, NodeId nodeId,
      String nodeHttpAddress, Resource resource, Priority priority,
      Token containerToken) {
    Container container = Records.newRecord(Container.class);
    container.setId(containerId);
    container.setNodeId(nodeId);
    container.setNodeHttpAddress(nodeHttpAddress);
    container.setResource(resource);
    container.setPriority(priority);
    container.setContainerToken(containerToken);
    return container;
  }

  /**
   * Get the globally unique identifier for the container.
   * @return globally unique identifier for the container
   */
  @Public
  @Stable
  public abstract ContainerId getId();
  
  @Private
  @Unstable
  public abstract void setId(ContainerId id);

  /**
   * Get the identifier of the node on which the container is allocated.
   * @return identifier of the node on which the container is allocated
   */
  @Public
  @Stable
  public abstract NodeId getNodeId();
  
  @Private
  @Unstable
  public abstract void setNodeId(NodeId nodeId);
  
  /**
   * Get the http uri of the node on which the container is allocated.
   * @return http uri of the node on which the container is allocated
   */
  @Public
  @Stable
  public abstract String getNodeHttpAddress();
  
  @Private
  @Unstable
  public abstract void setNodeHttpAddress(String nodeHttpAddress);
  
  /**
   * Get the <code>Resource</code> allocated to the container.
   * @return <code>Resource</code> allocated to the container
   */
  @Public
  @Stable
  public abstract Resource getResource();
  
  @Private
  @Unstable
  public abstract void setResource(Resource resource);

  /**
   * Get the <code>Priority</code> at which the <code>Container</code> was
   * allocated.
   * @return <code>Priority</code> at which the <code>Container</code> was
   *         allocated
   */
  @Public
  @Stable
  public abstract Priority getPriority();
  
  @Private
  @Unstable
  public abstract void setPriority(Priority priority);
  
  /**
   * Get the <code>ContainerToken</code> for the container.
   * <p><code>ContainerToken</code> is the security token used by the framework
   * to verify authenticity of any <code>Container</code>.</p>
   *
   * <p>The <code>ResourceManager</code>, on container allocation provides a
   * secure token which is verified by the <code>NodeManager</code> on
   * container launch.</p>
   *
   * <p>Applications do not need to care about <code>ContainerToken</code>, they
   * are transparently handled by the framework - the allocated
   * <code>Container</code> includes the <code>ContainerToken</code>.</p>
   *
   * @see ApplicationMasterProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
   * @see ContainerManagementProtocol#startContainers(org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest)
   *
   * @return <code>ContainerToken</code> for the container
   */
  @Public
  @Stable
  public abstract Token getContainerToken();
  
  @Private
  @Unstable
  public abstract void setContainerToken(Token containerToken);
}
