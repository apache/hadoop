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

package org.apache.hadoop.yarn.server.resourcemanager.resourcetracker;


import java.util.List;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Application;

/**
 * Node managers information on available resources 
 * and other static information.
 *
 */
public interface NodeInfo {
  /**
   * the node id of of this node.
   * @return the node id of this node.
   */
  public NodeId getNodeID();
  
  /**
   * the hostname of this node
   * @return hostname of this node
   */
  public String getNodeHostName();
  
  /**
   * the command port for this node
   * @return command port for this node
   */
  public int getCommandPort();
  
  /**
   * the http port for this node
   * @return http port for this node
   */
  public int getHttpPort();


  /**
   * the ContainerManager address for this node.
   * @return the ContainerManager address for this node.
   */
  public String getNodeAddress();
  
  /**
   * the http-Address for this node.
   * @return the http-url address for this node
   */
  public String getHttpAddress();
  
  /**
   * the health-status for this node
   * @return the health-status for this node.
   */
  public NodeHealthStatus getNodeHealthStatus();
  
  /**
   * the total available resource.
   * @return the total available resource.
   */
  public org.apache.hadoop.yarn.api.records.Resource getTotalCapability();
  
  /**
   * The rack name for this node manager.
   * @return the rack name.
   */
  public String getRackName();
  
  /**
   * the {@link Node} information for this node.
   * @return {@link Node} information for this node.
   */
  public Node getNode();
  
  /**
   * the available resource for this node.
   * @return the available resource this node.
   */
  public org.apache.hadoop.yarn.api.records.Resource getAvailableResource();
  
  /**
   * used resource on this node.
   * @return the used resource on this node.
   */
  public org.apache.hadoop.yarn.api.records.Resource getUsedResource();
  
  /**
   * The current number of containers for this node
   * @return the number of containers
   */
  public int getNumContainers();

  /**
   * Inform the node of allocated containers
   * @param applicationId the application id 
   * @param containers the list of containers
   */
  public void allocateContainer(ApplicationId applicationId,
      List<Container> containers);
  
  /**
   * Get running containers on this node.
   * @return running containers
   */
  public List<Container> getRunningContainers();
  
  /**
   * Get application which has a reserved container on this node.
   * @return application which has a reserved container on this node
   */
  public Application getReservedApplication();

  /**
   * Get reserved resource on this node.
   * @return reserved resource on this node
   */
  Resource getReservedResource();

  /**
   * Reserve resources on this node for a given application
   * @param application application for which to reserve
   * @param priority application priority
   * @param resource reserved resource
   */
  public void reserveResource(Application application, Priority priority, 
      Resource resource);

  /**
   * Unreserve resource on this node for a given application
   * @param application application for which to unreserve
   * @param priority application priority
   */
  public void unreserveResource(Application application, Priority priority);
  
}