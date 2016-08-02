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
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.api.types.RoleStatistics;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedExportsSet;
import org.apache.slider.server.appmaster.web.rest.RestPaths;
import org.apache.slider.server.services.utility.PatternValidator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link StateAccessForProviders}, which means
 * state access for providers, web UI and IPC/REST views.
 */
public class ProviderAppState implements StateAccessForProviders {


  private final Map<String, PublishedConfigSet> publishedConfigSets =
      new ConcurrentHashMap<>(5);
  private final PublishedExportsSet publishedExportsSets = new PublishedExportsSet();
  private static final PatternValidator validator = new PatternValidator(
      RestPaths.PUBLISHED_CONFIGURATION_SET_REGEXP);
  private String applicationName;

  private final AppState appState;

  public ProviderAppState(String applicationName, AppState appState) {
    this.appState = appState;
    this.applicationName = applicationName;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  @Override
  public String getApplicationName() {
    return applicationName;
  }

  @Override
  public PublishedConfigSet getPublishedSliderConfigurations() {
    return getOrCreatePublishedConfigSet(RestPaths.SLIDER_CONFIGSET);
  }

  @Override
  public PublishedExportsSet getPublishedExportsSet() {
    return publishedExportsSets;
  }

  @Override
  public PublishedConfigSet getPublishedConfigSet(String name) {
    return publishedConfigSets.get(name);
  }

  @Override
  public PublishedConfigSet getOrCreatePublishedConfigSet(String name) {
    PublishedConfigSet set = publishedConfigSets.get(name);
    if (set == null) {
      validator.validate(name);
      synchronized (publishedConfigSets) {
        // synchronized double check to ensure that there is never an overridden
        // config set created
        set = publishedConfigSets.get(name);
        if (set == null) {
          set = new PublishedConfigSet();
          publishedConfigSets.put(name, set);
        }
      }
    }
    return set;
  }

  @Override
  public List<String> listConfigSets() {

    synchronized (publishedConfigSets) {
      List<String> sets = new ArrayList<>(publishedConfigSets.keySet());
      return sets;
    }
  }

  @Override
  public Map<Integer, RoleStatus> getRoleStatusMap() {
    return appState.getRoleStatusMap();
  }


  @Override
  public Map<ContainerId, RoleInstance> getFailedContainers() {
    return appState.getFailedContainers();
  }

  @Override
  public Map<ContainerId, RoleInstance> getLiveContainers() {
    return appState.getLiveContainers();
  }

  @Override
  public ClusterDescription getClusterStatus() {
    return appState.getClusterStatus();
  }

  @Override
  public ConfTreeOperations getResourcesSnapshot() {
    return appState.getResourcesSnapshot();
  }

  @Override
  public ConfTreeOperations getAppConfSnapshot() {
    return appState.getAppConfSnapshot();
  }

  @Override
  public ConfTreeOperations getInternalsSnapshot() {
    return appState.getInternalsSnapshot();
  }

  @Override
  public boolean isApplicationLive() {
    return appState.isApplicationLive();
  }

  @Override
  public long getSnapshotTime() {
    return appState.getSnapshotTime();
  }

  @Override
  public AggregateConf getInstanceDefinitionSnapshot() {
    return appState.getInstanceDefinitionSnapshot();
  }
  
  @Override
  public AggregateConf getUnresolvedInstanceDefinition() {
    return appState.getUnresolvedInstanceDefinition();
  }

  @Override
  public RoleStatus lookupRoleStatus(int key) {
    return appState.lookupRoleStatus(key);
  }

  @Override
  public RoleStatus lookupRoleStatus(Container c) throws YarnRuntimeException {
    return appState.lookupRoleStatus(c);
  }

  @Override
  public RoleStatus lookupRoleStatus(String name) throws YarnRuntimeException {
    return appState.lookupRoleStatus(name);
  }

  @Override
  public List<RoleInstance> cloneOwnedContainerList() {
    return appState.cloneOwnedContainerList();
  }

  @Override
  public int getNumOwnedContainers() {
    return appState.getNumOwnedContainers();
  }

  @Override
  public RoleInstance getOwnedContainer(ContainerId id) {
    return appState.getOwnedContainer(id);
  }

  @Override
  public RoleInstance getOwnedContainer(String id) throws NoSuchNodeException {
    return appState.getOwnedInstanceByContainerID(id);
  }

  @Override
  public List<RoleInstance> cloneLiveContainerInfoList() {
    return appState.cloneLiveContainerInfoList();
  }

  @Override
  public RoleInstance getLiveInstanceByContainerID(String containerId) throws
      NoSuchNodeException {
    return appState.getLiveInstanceByContainerID(containerId);
  }

  @Override
  public List<RoleInstance> getLiveInstancesByContainerIDs(Collection<String> containerIDs) {
    return appState.getLiveInstancesByContainerIDs(containerIDs);
  }

  @Override
  public ClusterDescription refreshClusterStatus() {
    return appState.refreshClusterStatus();
  }

  @Override
  public List<RoleStatus> cloneRoleStatusList() {
    return appState.cloneRoleStatusList();
  }

  @Override
  public ApplicationLivenessInformation getApplicationLivenessInformation() {
    return appState.getApplicationLivenessInformation();
  }

  @Override
  public Map<String, Integer> getLiveStatistics() {
    return appState.getLiveStatistics();
  }

  @Override
  public Map<String, ComponentInformation> getComponentInfoSnapshot() {
    return appState.getComponentInfoSnapshot();
  }

  @Override
  public Map<String, Map<String, ClusterNode>> getRoleClusterNodeMapping() {
    return appState.createRoleToClusterNodeMap();
  }

  @Override
  public List<RoleInstance> enumLiveInstancesInRole(String role) {
    List<RoleInstance> nodes = new ArrayList<>();
    Collection<RoleInstance> allRoleInstances = cloneLiveContainerInfoList();
    for (RoleInstance node : allRoleInstances) {
      if (role.isEmpty() || role.equals(node.role)) {
        nodes.add(node);
      }
    }
    return nodes;
  }

  @Override
  public List<RoleInstance> lookupRoleContainers(String component) {
    RoleStatus roleStatus = lookupRoleStatus(component);
    List<RoleInstance> ownedContainerList = cloneOwnedContainerList();
    List<RoleInstance> matching = new ArrayList<>(ownedContainerList.size());
    int roleId = roleStatus.getPriority();
    for (RoleInstance instance : ownedContainerList) {
      if (instance.roleId == roleId) {
        matching.add(instance);
      }
    }
    return matching;
  }
  
  @Override
  public ComponentInformation getComponentInformation(String component) {
    RoleStatus roleStatus = lookupRoleStatus(component);
    ComponentInformation info = roleStatus.serialize();
    List<RoleInstance> containers = lookupRoleContainers(component);
    info.containers = new ArrayList<>(containers.size());
    for (RoleInstance container : containers) {
      info.containers.add(container.id);
    }
    return info;
  }

  @Override
  public Map<String, NodeInformation> getNodeInformationSnapshot() {
    return appState.getRoleHistory()
      .getNodeInformationSnapshot(appState.buildNamingMap());
  }

  @Override
  public NodeInformation getNodeInformation(String hostname) {
    return appState.getRoleHistory()
      .getNodeInformation(hostname, appState.buildNamingMap());
  }

  @Override
  public RoleStatistics getRoleStatistics() {
    return appState.getRoleStatistics();
  }
}
