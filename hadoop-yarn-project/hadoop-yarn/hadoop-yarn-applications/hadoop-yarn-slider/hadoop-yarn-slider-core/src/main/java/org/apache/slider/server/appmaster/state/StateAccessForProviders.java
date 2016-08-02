/*
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

package org.apache.slider.server.appmaster.state;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.api.types.RoleStatistics;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedExportsSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The methods to offer state access to the providers and other parts of
 * the system which want read-only access to the state.
 */
public interface StateAccessForProviders {

  /**
   * Get a map of role status entries by role Id
   * @return the map of currently defined roles.
   */
  Map<Integer, RoleStatus> getRoleStatusMap();

  /**
   * Get the name of the application
   * @return the name
   */
  String getApplicationName();

  /**
   * Get the published configurations
   * @return the configuration set
   */
  PublishedConfigSet getPublishedSliderConfigurations();

  /**
   * Get the published exports set
   * @return
   */
  PublishedExportsSet getPublishedExportsSet();

  /**
   * Get a named published config set
   * @param name name to look up
   * @return the instance or null
   */
  PublishedConfigSet getPublishedConfigSet(String name);

  /**
   * Get a named published config set, creating it if need be.
   * @param name name to look up
   * @return the instance -possibly a new one
   */
  PublishedConfigSet getOrCreatePublishedConfigSet(String name);

  /**
   * List the config sets -this takes a clone of the current set
   * @return a list of config sets
   */
  List<String> listConfigSets();

  /**
   * Get a map of all the failed containers
   * @return map of recorded failed containers
   */
  Map<ContainerId, RoleInstance> getFailedContainers();

  /**
   * Get the live containers.
   * 
   * @return the live nodes
   */
  Map<ContainerId, RoleInstance> getLiveContainers();

  /**
   * Get the current cluster description 
   * @return the actual state of the cluster
   */
  ClusterDescription getClusterStatus();

  /**
   * Get at the snapshot of the resource config
   * Changes here do not affect the application state.
   * @return the most recent settings
   */
  ConfTreeOperations getResourcesSnapshot();

  /**
   * Get at the snapshot of the appconf config
   * Changes here do not affect the application state.
   * @return the most recent settings
   */
  ConfTreeOperations getAppConfSnapshot();

  /**
   * Get at the snapshot of the internals config.
   * Changes here do not affect the application state.
   * @return the internals settings
   */

  ConfTreeOperations getInternalsSnapshot();

  /**
   * Flag set to indicate the application is live -this only happens
   * after the buildInstance operation
   */
  boolean isApplicationLive();

  long getSnapshotTime();

  /**
   * Get a snapshot of the entire aggregate configuration
   * @return the aggregate configuration
   */
  AggregateConf getInstanceDefinitionSnapshot();

  /**
   * Get the desired/unresolved value
   * @return unresolved
   */
  AggregateConf getUnresolvedInstanceDefinition();

  /**
   * Look up a role from its key -or fail 
   *
   * @param key key to resolve
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  RoleStatus lookupRoleStatus(int key);

  /**
   * Look up a role from its key -or fail 
   *
   * @param c container in a role
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  RoleStatus lookupRoleStatus(Container c) throws YarnRuntimeException;

  /**
   * Look up a role from its key -or fail 
   *
   * @param name container in a role
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  RoleStatus lookupRoleStatus(String name) throws YarnRuntimeException;

  /**
   * Clone a list of active containers
   * @return the active containers at the time
   * the call was made
   */
  List<RoleInstance> cloneOwnedContainerList();

  /**
   * Get the number of active containers
   * @return the number of active containers the time the call was made
   */
  int getNumOwnedContainers();

  /**
   * Get any active container with the given ID
   * @param id container Id
   * @return the active container or null if it is not found
   */
  RoleInstance getOwnedContainer(ContainerId id);

  /**
   * Get any active container with the given ID
   * @param id container Id
   * @return the active container or null if it is not found
   */
  RoleInstance getOwnedContainer(String id) throws NoSuchNodeException;

  /**
   * Create a clone of the list of live cluster nodes.
   * @return the list of nodes, may be empty
   */
  List<RoleInstance> cloneLiveContainerInfoList();

  /**
   * Get the {@link RoleInstance} details on a container.
   * This is an O(n) operation
   * @param containerId the container ID
   * @return null if there is no such node
   * @throws NoSuchNodeException if the node cannot be found
   */
  RoleInstance getLiveInstanceByContainerID(String containerId)
    throws NoSuchNodeException;

  /**
   * Get the details on a list of instaces referred to by ID.
   * Unknown nodes are not returned
   * <i>Important: the order of the results are undefined</i>
   * @param containerIDs the containers
   * @return list of instances
   */
  List<RoleInstance> getLiveInstancesByContainerIDs(
    Collection<String> containerIDs);

  /**
   * Update the cluster description with anything interesting
   */
  ClusterDescription refreshClusterStatus();

  /**
   * Get a deep clone of the role status list. Concurrent events may mean this
   * list (or indeed, some of the role status entries) may be inconsistent
   * @return a snapshot of the role status entries
   */
  List<RoleStatus> cloneRoleStatusList();

  /**
   * get application liveness information
   * @return a snapshot of the current liveness information
   */
  ApplicationLivenessInformation getApplicationLivenessInformation();

  /**
   * Get the live statistics map
   * @return a map of statistics values, defined in the {@link StatusKeys}
   * keylist.
   */
  Map<String, Integer> getLiveStatistics();

  /**
   * Get a snapshot of component information.
   * <p>
   *   This does <i>not</i> include any container list, which 
   *   is more expensive to create.
   * @return a map of current role status values.
   */
  Map<String, ComponentInformation> getComponentInfoSnapshot();

  /**
   * Find out about the nodes for specific roles
   * @return 
   */
  Map<String, Map<String, ClusterNode>> getRoleClusterNodeMapping();

  /**
   * Enum all role instances by role.
   * @param role role, or "" for all roles
   * @return a list of instances, may be empty
   */
  List<RoleInstance> enumLiveInstancesInRole(String role);

  /**
   * Look up all containers of a specific component name 
   * @param component component/role name
   * @return list of instances. This is a snapshot
   */
  List<RoleInstance> lookupRoleContainers(String component);

  /**
   * Get the JSON serializable information about a component
   * @param component component to look up
   * @return a structure describing the component.
   */
  ComponentInformation getComponentInformation(String component);


  /**
   * Get a clone of the nodemap.
   * The instances inside are not cloned
   * @return a possibly empty map of hostname top info
   */
  Map<String, NodeInformation> getNodeInformationSnapshot();

  /**
   * get information on a node
   * @param hostname hostname to look up
   * @return the information, or null if there is no information held.
   */
  NodeInformation getNodeInformation(String hostname);

  /**
   * Get the aggregate statistics across all roles
   * @return role statistics
   */
  RoleStatistics getRoleStatistics();
}
