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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.ServiceApiConstants;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.api.proto.Messages.ComponentCountProto;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.ApplicationState;
import org.apache.slider.api.resource.Component;
import org.apache.slider.api.resource.ConfigFile;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.RoleStatistics;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.exceptions.SliderInternalStateException;
import org.apache.slider.core.exceptions.TriggerClusterTeardownException;
import org.apache.slider.core.zk.ZKIntegration;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.management.MetricsConstants;
import org.apache.slider.server.appmaster.metrics.SliderMetrics;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.apache.slider.server.appmaster.operations.UpdateBlacklistOperation;
import org.apache.slider.server.appmaster.timelineservice.ServiceTimelinePublisher;
import org.apache.slider.util.ServiceApiUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.registry.client.api.RegistryConstants.DEFAULT_REGISTRY_ZK_QUORUM;
import static org.apache.hadoop.registry.client.api.RegistryConstants.KEY_DNS_DOMAIN;
import static org.apache.hadoop.registry.client.api.RegistryConstants.KEY_REGISTRY_ZK_QUORUM;
import static org.apache.slider.api.ResourceKeys.*;
import static org.apache.slider.api.ServiceApiConstants.*;
import static org.apache.slider.api.StateValues.*;
import static org.apache.slider.api.resource.ApplicationState.STARTED;

/**
 * The model of all the ongoing state of a Slider AM.
 *
 * concurrency rules: any method which begins with <i>build</i>
 * is not synchronized and intended to be used during
 * initialization.
 */
public class AppState {
  protected static final Logger log =
    LoggerFactory.getLogger(AppState.class);
  
  private final AbstractClusterServices recordFactory;

  private final MetricsAndMonitoring metricsAndMonitoring;
  /**
   * Flag set to indicate the application is live -this only happens
   * after the buildInstance operation
   */
  private boolean applicationLive = false;

  private Application app;

  // priority_id -> RoleStatus
  private final Map<Integer, RoleStatus> roleStatusMap =
    new ConcurrentSkipListMap<>();

  // component_name -> ProviderRole
  private final Map<String, ProviderRole> roles =
    new ConcurrentHashMap<>();

  private final ConcurrentSkipListMap<Integer, ProviderRole> rolePriorityMap =
    new ConcurrentSkipListMap<>();

  /**
   * Hash map of the containers we have. This includes things that have
   * been allocated but are not live; it is a superset of the live list
   */
  private final ConcurrentMap<ContainerId, RoleInstance> ownedContainers =
    new ConcurrentHashMap<>();

  /**
   * Hash map of the containers we have released, but we
   * are still awaiting acknowledgements on. Any failure of these
   * containers is treated as a successful outcome
   */
  private final ConcurrentMap<ContainerId, Container> containersBeingReleased =
    new ConcurrentHashMap<>();

  /**
   * Map of requested nodes. This records the command used to start it,
   * resources, etc. When container started callback is received,
   * the node is promoted from here to the containerMap
   */
  private final Map<ContainerId, RoleInstance> startingContainers =
    new ConcurrentHashMap<>();

  /**
   * List of completed nodes. This isn't kept in the CD as it gets too
   * big for the RPC responses. Indeed, we should think about how deep to get this
   */
  private final Map<ContainerId, RoleInstance> completedContainers
    = new ConcurrentHashMap<>();

  /**
   * Nodes that failed to start.
   * Again, kept out of the CD
   */
  private final Map<ContainerId, RoleInstance> failedContainers =
    new ConcurrentHashMap<>();

  /**
   * Nodes that came assigned to a role above that
   * which were asked for -this appears to happen
   */
  private final Set<ContainerId> surplusContainers = new HashSet<>();

  /**
   * Map of containerID to cluster nodes, for status reports.
   * Access to this should be synchronized on the clusterDescription
   */
  private final Map<ContainerId, RoleInstance> liveNodes =
    new ConcurrentHashMap<>();
  private final AtomicInteger completionOfNodeNotInLiveListEvent =
    new AtomicInteger();
  private final AtomicInteger completionOfUnknownContainerEvent =
    new AtomicInteger();

  /**
   * limits of container core numbers in this queue
   */
  private int containerMaxCores;
  private int containerMinCores;

  /**
   * limits of container memory in this queue
   */
  private int containerMaxMemory;
  private int containerMinMemory;

  private RoleHistory roleHistory;
  private long startTimeThreshold;

  private int failureThreshold = 10;
  private int nodeFailureThreshold = 3;

  private String logServerURL = "";
  public Map<String, String> globalTokens = new HashMap<>();
  /**
   * Selector of containers to release; application wide.
   */
  private ContainerReleaseSelector containerReleaseSelector;
  private Resource minResource;
  private Resource maxResource;

  private SliderMetrics appMetrics;

  private ServiceTimelinePublisher serviceTimelinePublisher;

  // A cache for loading config files from remote such as hdfs
  public LoadingCache<ConfigFile, Object> configFileCache = null;

  /**
   * Create an instance
   * @param recordFactory factory for YARN records
   * @param metricsAndMonitoring metrics and monitoring services
   */
  public AppState(AbstractClusterServices recordFactory,
      MetricsAndMonitoring metricsAndMonitoring) {
    Preconditions.checkArgument(recordFactory != null, "null recordFactory");
    Preconditions.checkArgument(metricsAndMonitoring != null, "null metricsAndMonitoring");
    this.recordFactory = recordFactory;
    this.metricsAndMonitoring = metricsAndMonitoring;
  }


  public Map<Integer, RoleStatus> getRoleStatusMap() {
    return roleStatusMap;
  }
  
  protected Map<String, ProviderRole> getRoleMap() {
    return roles;
  }

  public Map<Integer, ProviderRole> getRolePriorityMap() {
    return rolePriorityMap;
  }

  private Map<ContainerId, RoleInstance> getStartingContainers() {
    return startingContainers;
  }

  private Map<ContainerId, RoleInstance> getCompletedContainers() {
    return completedContainers;
  }

  public Map<ContainerId, RoleInstance> getFailedContainers() {
    return failedContainers;
  }

  public Map<ContainerId, RoleInstance> getLiveContainers() {
    return liveNodes;
  }

  /**
   * Get the current view of the cluster status.
   * This is read-only
   * to the extent that changes here do not trigger updates in the
   * application state. 
   * @return the cluster status
   */
  public synchronized Application getClusterStatus() {
    return app;
  }

  /**
   * Get the role history of the application
   * @return the role history
   */
  @VisibleForTesting
  public RoleHistory getRoleHistory() {
    return roleHistory;
  }

  @VisibleForTesting
  public void setRoleHistory(RoleHistory roleHistory) {
    this.roleHistory = roleHistory;
  }

  /**
   * Get the path used for history files
   * @return the directory used for history files
   */
  @VisibleForTesting
  public Path getHistoryPath() {
    return roleHistory.getHistoryPath();
  }

  /**
   * Set the container limits -the min and max values for
   * resource requests. All requests must be multiples of the min
   * values.
   * @param minMemory min memory MB
   * @param maxMemory maximum memory
   * @param minCores min v core count
   * @param maxCores maximum cores
   */
  public void setContainerLimits(int minMemory,int maxMemory, int minCores, int maxCores) {
    containerMinCores = minCores;
    containerMaxCores = maxCores;
    containerMinMemory = minMemory;
    containerMaxMemory = maxMemory;
    minResource = recordFactory.newResource(containerMinMemory, containerMinCores);
    maxResource = recordFactory.newResource(containerMaxMemory, containerMaxCores);
  }

  public boolean isApplicationLive() {
    return applicationLive;
  }


  public synchronized void buildInstance(AppStateBindingInfo binding)
      throws BadClusterStateException, BadConfigException, IOException {
    binding.validate();
    containerReleaseSelector = binding.releaseSelector;

    // set the cluster specification (once its dependency the client properties
    // is out the way
    this.app = binding.application;
    appMetrics = SliderMetrics.register(app.getName(),
        "Metrics for service");
    appMetrics.tag("type", "Metrics type [component or service]", "service");
    appMetrics.tag("appId", "Application id for service", app.getId());

    org.apache.slider.api.resource.Configuration conf = app.getConfiguration();
    startTimeThreshold =
        conf.getPropertyLong(InternalKeys.INTERNAL_CONTAINER_FAILURE_SHORTLIFE,
            InternalKeys.DEFAULT_INTERNAL_CONTAINER_FAILURE_SHORTLIFE);
    failureThreshold = conf.getPropertyInt(CONTAINER_FAILURE_THRESHOLD,
        DEFAULT_CONTAINER_FAILURE_THRESHOLD);
    nodeFailureThreshold = conf.getPropertyInt(NODE_FAILURE_THRESHOLD,
        DEFAULT_NODE_FAILURE_THRESHOLD);
    initGlobalTokensForSubstitute(binding);

    // build the initial component list
    Collection<Component> sortedComponents = ServiceApiUtil
        .sortByDependencies(app.getComponents());
    int priority = 1;
    for (Component component : sortedComponents) {
      priority = getNewPriority(priority);
      String name = component.getName();
      if (roles.containsKey(name)) {
        continue;
      }
      log.info("Adding component: " + name);
      createComponent(name, component, priority++);
    }

    //then pick up the requirements
//    buildRoleRequirementsFromResources();

    // set up the role history
    roleHistory = new RoleHistory(roleStatusMap.values(), recordFactory);
    roleHistory.onStart(binding.fs, binding.historyPath);
    // trigger first node update
    roleHistory.onNodesUpdated(binding.nodeReports);
    //rebuild any live containers
    rebuildModelFromRestart(binding.liveContainers);

    // any am config options to pick up
    logServerURL = binding.serviceConfig.get(YarnConfiguration.YARN_LOG_SERVER_URL, "");
    //mark as live
    applicationLive = true;
    app.setState(STARTED);
    createConfigFileCache(binding.fs);
  }

  private void initGlobalTokensForSubstitute(AppStateBindingInfo binding)
      throws IOException {
    // ZK
    globalTokens.put(ServiceApiConstants.CLUSTER_ZK_QUORUM,
        binding.serviceConfig
            .getTrimmed(KEY_REGISTRY_ZK_QUORUM, DEFAULT_REGISTRY_ZK_QUORUM));
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    globalTokens
        .put(SERVICE_ZK_PATH, ZKIntegration.mkClusterPath(user, app.getName()));

    globalTokens.put(ServiceApiConstants.USER, user);
    String dnsDomain = binding.serviceConfig.getTrimmed(KEY_DNS_DOMAIN);
    if (dnsDomain != null && !dnsDomain.isEmpty()) {
      globalTokens.put(ServiceApiConstants.DOMAIN, dnsDomain);
    }
    // HDFS
    String clusterFs = binding.serviceConfig.getTrimmed(FS_DEFAULT_NAME_KEY);
    if (clusterFs != null && !clusterFs.isEmpty()) {
      globalTokens.put(ServiceApiConstants.CLUSTER_FS_URI, clusterFs);
      globalTokens.put(ServiceApiConstants.CLUSTER_FS_HOST,
          URI.create(clusterFs).getHost());
    }
    globalTokens.put(SERVICE_HDFS_DIR, binding.serviceHdfsDir);
    // service name
    globalTokens.put(SERVICE_NAME_LC, app.getName().toLowerCase());
    globalTokens.put(SERVICE_NAME, app.getName());
  }

  private void createConfigFileCache(final FileSystem fileSystem) {
    this.configFileCache =
        CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES)
            .build(new CacheLoader<ConfigFile, Object>() {
              @Override public Object load(ConfigFile key) throws Exception {
                switch (key.getType()) {
                case HADOOP_XML:
                  try (FSDataInputStream input = fileSystem
                      .open(new Path(key.getSrcFile()))) {
                    org.apache.hadoop.conf.Configuration confRead =
                        new org.apache.hadoop.conf.Configuration(false);
                    confRead.addResource(input);
                    Map<String, String> map = new HashMap<>(confRead.size());
                    for (Map.Entry<String, String> entry : confRead) {
                      map.put(entry.getKey(), entry.getValue());
                    }
                    return map;
                  }
                case TEMPLATE:
                  try (FSDataInputStream fileInput = fileSystem
                      .open(new Path(key.getSrcFile()))) {
                    return IOUtils.toString(fileInput);
                  }
                default:
                  return null;
                }
              }
            });
  }

  public ProviderRole createComponent(String name, Component component,
      int priority) throws BadConfigException {
    org.apache.slider.api.resource.Configuration conf =
        component.getConfiguration();
    long placementTimeout = conf.getPropertyLong(PLACEMENT_ESCALATE_DELAY,
        DEFAULT_PLACEMENT_ESCALATE_DELAY_SECONDS);
    long placementPolicy = conf.getPropertyLong(COMPONENT_PLACEMENT_POLICY,
        PlacementPolicy.DEFAULT);
    int threshold = conf.getPropertyInt(NODE_FAILURE_THRESHOLD,
        nodeFailureThreshold);
    String label = conf.getProperty(YARN_LABEL_EXPRESSION,
        DEF_YARN_LABEL_EXPRESSION);
    ProviderRole newRole =
        new ProviderRole(name, priority, (int)placementPolicy, threshold,
            placementTimeout, label, component);
    buildRole(newRole, component);
    log.info("Created a new role " + newRole);
    return newRole;
  }

  @VisibleForTesting
  public synchronized void updateComponents(Map<String, Long>
      componentCounts) throws BadConfigException {
    for (Component component : app.getComponents()) {
      if (componentCounts.containsKey(component.getName())) {
        long count = componentCounts.get(component.getName());
        component.setNumberOfContainers(count);
        ProviderRole role = roles.get(component.getName());
        if (role != null && roleStatusMap.get(role.id) != null) {
          setDesiredContainers(roleStatusMap.get(role.id), (int) count);
        }
      }
    }
  }

  public synchronized void updateComponents(
      Messages.FlexComponentsRequestProto requestProto)
      throws BadConfigException {
    Map<String, Long> componentCounts = new HashMap<>();
    for (ComponentCountProto componentCount : requestProto
        .getComponentsList()) {
      componentCounts.put(componentCount.getName(), componentCount
          .getNumberOfContainers());
    }
    updateComponents(componentCounts);
  }

  /**
   * build the role requirements from the cluster specification
   * @return a list of any dynamically added provider roles
   */

//  private List<ProviderRole> buildRoleRequirementsFromResources()
//      throws BadConfigException {
//
//    List<ProviderRole> newRoles = new ArrayList<>(0);
//
//    // now update every role's desired count.
//    // if there are no instance values, that role count goes to zero
//    // Add all the existing roles
//    // component name -> number of containers
//    Map<String, Integer> groupCounts = new HashMap<>();
//
//    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
//      if (roleStatus.isExcludeFromFlexing()) {
//        // skip inflexible roles, e.g AM itself
//        continue;
//      }
//      long currentDesired = roleStatus.getDesired();
//      String role = roleStatus.getName();
//      String roleGroup = roleStatus.getGroup();
//      Component component = roleStatus.getProviderRole().component;
//      int desiredInstanceCount = component.getNumberOfContainers().intValue();
//
//      int newDesired = desiredInstanceCount;
//      if (component.getUniqueComponentSupport()) {
//        Integer groupCount = 0;
//        if (groupCounts.containsKey(roleGroup)) {
//          groupCount = groupCounts.get(roleGroup);
//        }
//
//        newDesired = desiredInstanceCount - groupCount;
//
//        if (newDesired > 0) {
//          newDesired = 1;
//          groupCounts.put(roleGroup, groupCount + newDesired);
//        } else {
//          newDesired = 0;
//        }
//      }
//
//      if (newDesired == 0) {
//        log.info("Role {} has 0 instances specified", role);
//      }
//      if (currentDesired != newDesired) {
//        log.info("Role {} flexed from {} to {}", role, currentDesired,
//            newDesired);
//        setDesiredContainers(roleStatus, newDesired);
//      }
//    }
//
//    log.info("Counts per component: " + groupCounts);
//    // now the dynamic ones. Iterate through the the cluster spec and
//    // add any role status entries not in the role status
//
//    List<RoleStatus> list = new ArrayList<>(getRoleStatusMap().values());
//    for (RoleStatus roleStatus : list) {
//      String name = roleStatus.getName();
//      Component component = roleStatus.getProviderRole().component;
//      if (roles.containsKey(name)) {
//        continue;
//      }
//      if (component.getUniqueComponentSupport()) {
//        // THIS NAME IS A GROUP
//        int desiredInstanceCount = component.getNumberOfContainers().intValue();
//        Integer groupCount = 0;
//        if (groupCounts.containsKey(name)) {
//          groupCount = groupCounts.get(name);
//        }
//        log.info("Component " + component.getName() + ", current count = "
//            + groupCount + ", desired count = " + desiredInstanceCount);
//        for (int i = groupCount + 1; i <= desiredInstanceCount; i++) {
//          int priority = roleStatus.getPriority();
//          // this is a new instance of an existing group
//          String newName = String.format("%s%d", name, i);
//          int newPriority = getNewPriority(priority + i - 1);
//          log.info("Adding new role {}", newName);
//          ProviderRole dynamicRole =
//              createComponent(newName, name, component, newPriority);
//          RoleStatus newRole = buildRole(dynamicRole);
//          incDesiredContainers(newRole);
//          log.info("New role {}", newRole);
//          if (roleHistory != null) {
//            roleHistory.addNewRole(newRole);
//          }
//          newRoles.add(dynamicRole);
//        }
//      } else {
//        // this is a new value
//        log.info("Adding new role {}", name);
//        ProviderRole dynamicRole =
//            createComponent(name, name, component, roleStatus.getPriority());
//        RoleStatus newRole = buildRole(dynamicRole);
//        incDesiredContainers(roleStatus,
//            component.getNumberOfContainers().intValue());
//        log.info("New role {}", newRole);
//        if (roleHistory != null) {
//          roleHistory.addNewRole(newRole);
//        }
//        newRoles.add(dynamicRole);
//      }
//    }
//    // and fill in all those roles with their requirements
//    buildRoleResourceRequirements();
//
//    return newRoles;
//  }

  private int getNewPriority(int start) {
    if (!rolePriorityMap.containsKey(start)) {
      return start;
    }
    return rolePriorityMap.lastKey() + 1;
  }

  /**
   * Add knowledge of a role.
   * This is a build-time operation that is not synchronized, and
   * should be used while setting up the system state -before servicing
   * requests.
   * @param providerRole role to add
   * @return the role status built up
   * @throws BadConfigException if a role of that priority already exists
   */
  public RoleStatus buildRole(ProviderRole providerRole, Component component)
      throws BadConfigException {
    // build role status map
    int priority = providerRole.id;
    if (roleStatusMap.containsKey(priority)) {
      throw new BadConfigException("Duplicate component priority Key: %s and %s",
          providerRole, roleStatusMap.get(priority));
    }
    RoleStatus roleStatus = new RoleStatus(providerRole);
    roleStatus.setResourceRequirements(buildResourceRequirements(roleStatus));
    long prev = roleStatus.getDesired();
    setDesiredContainers(roleStatus, component.getNumberOfContainers().intValue());
    log.info("Set desired containers for component " + component.getName() +
        " from " + prev + " to " + roleStatus.getDesired());
    roleStatusMap.put(priority, roleStatus);
    String name = providerRole.name;
    roles.put(name, providerRole);
    rolePriorityMap.put(priority, providerRole);
    // register its entries
    metricsAndMonitoring.addMetricSet(MetricsConstants.PREFIX_SLIDER_ROLES + name, roleStatus);
    return roleStatus;
  }

  /**
   * Look up the status entry of a role or raise an exception
   * @param key role ID
   * @return the status entry
   * @throws RuntimeException if the role cannot be found
   */
  public RoleStatus lookupRoleStatus(int key) {
    RoleStatus rs = getRoleStatusMap().get(key);
    if (rs == null) {
      throw new RuntimeException("Cannot find role for role ID " + key);
    }
    return rs;
  }

  /**
   * Look up the status entry of a container or raise an exception
   *
   * @param c container
   * @return the status entry
   * @throws RuntimeException if the role cannot be found
   */
  public RoleStatus lookupRoleStatus(Container c) {
    return lookupRoleStatus(ContainerPriority.extractRole(c));
  }


  /**
   * Look up a role in the map
   * @param name role name
   * @return the instance
   * @throws YarnRuntimeException if not found
   */
  public RoleStatus lookupRoleStatus(String name) throws YarnRuntimeException {
    ProviderRole providerRole = roles.get(name);
    if (providerRole == null) {
      throw new YarnRuntimeException("Unknown role " + name);
    }
    return lookupRoleStatus(providerRole.id);
  }


  /**
   * Clone the list of active (==owned) containers
   * @return the list of role instances representing all owned containers
   */
  public synchronized List<RoleInstance> cloneOwnedContainerList() {
    Collection<RoleInstance> values = ownedContainers.values();
    return new ArrayList<>(values);
  }

  /**
   * Get the number of active (==owned) containers
   * @return
   */
  public int getNumOwnedContainers() {
    return ownedContainers.size();
  }
  
  /**
   * Look up an active container: any container that the AM has, even
   * if it is not currently running/live
   */
  public RoleInstance getOwnedContainer(ContainerId id) {
    return ownedContainers.get(id);
  }

  /**
   * Remove an owned container
   * @param id container ID
   * @return the instance removed
   */
  private RoleInstance removeOwnedContainer(ContainerId id) {
    return ownedContainers.remove(id);
  }

  /**
   * set/update an owned container
   * @param id container ID
   * @param instance
   * @return
   */
  private RoleInstance putOwnedContainer(ContainerId id,
      RoleInstance instance) {
    return ownedContainers.put(id, instance);
  }

  /**
   * Clone the live container list. This is synchronized.
   * @return a snapshot of the live node list
   */
  public synchronized List<RoleInstance> cloneLiveContainerInfoList() {
    List<RoleInstance> allRoleInstances;
    Collection<RoleInstance> values = getLiveContainers().values();
    allRoleInstances = new ArrayList<>(values);
    return allRoleInstances;
  }

  /**
   * Lookup live instance by string value of container ID
   * @param containerId container ID as a string
   * @return the role instance for that container
   * @throws NoSuchNodeException if it does not exist
   */
  public synchronized RoleInstance getLiveInstanceByContainerID(String containerId)
      throws NoSuchNodeException {
    Collection<RoleInstance> nodes = getLiveContainers().values();
    return findNodeInCollection(containerId, nodes);
  }

  /**
   * Lookup owned instance by string value of container ID
   * @param containerId container ID as a string
   * @return the role instance for that container
   * @throws NoSuchNodeException if it does not exist
   */
  public synchronized RoleInstance getOwnedInstanceByContainerID(String containerId)
      throws NoSuchNodeException {
    Collection<RoleInstance> nodes = ownedContainers.values();
    return findNodeInCollection(containerId, nodes);
  }

  /**
   * Iterate through a collection of role instances to find one with a
   * specific (string) container ID
   * @param containerId container ID as a string
   * @param nodes collection
   * @return the found node 
   * @throws NoSuchNodeException if there was no match
   */
  private RoleInstance findNodeInCollection(String containerId,
      Collection<RoleInstance> nodes) throws NoSuchNodeException {
    RoleInstance found = null;
    for (RoleInstance node : nodes) {
      if (containerId.equals(node.id)) {
        found = node;
        break;
      }
    }
    if (found != null) {
      return found;
    } else {
      //at this point: no node
      throw new NoSuchNodeException("Unknown node: " + containerId);
    }
  }

  public synchronized List<RoleInstance> getLiveInstancesByContainerIDs(
    Collection<String> containerIDs) {
    //first, a hashmap of those containerIDs is built up
    Set<String> uuidSet = new HashSet<String>(containerIDs);
    List<RoleInstance> nodes = new ArrayList<RoleInstance>(uuidSet.size());
    Collection<RoleInstance> clusterNodes = getLiveContainers().values();

    for (RoleInstance node : clusterNodes) {
      if (uuidSet.contains(node.id)) {
        nodes.add(node);
      }
    }
    //at this point: a possibly empty list of nodes
    return nodes;
  }

  /**
   * Enum all nodes by role.
   * @param role role, or "" for all roles
   * @return a list of nodes, may be empty
   */
  public synchronized List<RoleInstance> enumLiveNodesInRole(String role) {
    List<RoleInstance> nodes = new ArrayList<RoleInstance>();
    Collection<RoleInstance> allRoleInstances = getLiveContainers().values();
    for (RoleInstance node : allRoleInstances) {
      if (role.isEmpty() || role.equals(node.role)) {
        nodes.add(node);
      }
    }
    return nodes;
  }

 
  /**
   * enum nodes by role ID, from either the owned or live node list
   * @param roleId role the container must be in
   * @param owned flag to indicate "use owned list" rather than the smaller
   * "live" list
   * @return a list of nodes, may be empty
   */
  public synchronized List<RoleInstance> enumNodesWithRoleId(int roleId,
      boolean owned) {
    List<RoleInstance> nodes = new ArrayList<RoleInstance>();
    Collection<RoleInstance> allRoleInstances;
    allRoleInstances = owned ? ownedContainers.values() : liveNodes.values();
    for (RoleInstance node : allRoleInstances) {
      if (node.roleId == roleId) {
        nodes.add(node);
      }
    }
    return nodes;
  }

  /**
   * Build an instance map.
   * @return the map of Role name to list of role instances
   */
  private synchronized Map<String, List<String>> createRoleToInstanceMap() {
    Map<String, List<String>> map = new HashMap<String, List<String>>();
    for (RoleInstance node : getLiveContainers().values()) {
      List<String> containers = map.get(node.role);
      if (containers == null) {
        containers = new ArrayList<String>();
        map.put(node.role, containers);
      }
      containers.add(node.id);
    }
    return map;
  }

  /**
   * Build a map of Component_name -> ContainerId -> ClusterNode
   * 
   * @return the map of Role name to list of Cluster Nodes
   */
  public synchronized Map<String, Map<String, ClusterNode>> createRoleToClusterNodeMap() {
    Map<String, Map<String, ClusterNode>> map = new HashMap<>();
    for (RoleInstance node : getLiveContainers().values()) {
      
      Map<String, ClusterNode> containers = map.get(node.role);
      if (containers == null) {
        containers = new HashMap<String, ClusterNode>();
        map.put(node.role, containers);
      }
      ClusterNode clusterNode = node.toClusterNode();
      containers.put(clusterNode.name, clusterNode);
    }
    return map;
  }

  /**
   * Notification called just before the NM is asked to 
   * start a container
   * @param container container to start
   * @param instance clusterNode structure
   */
  public void containerStartSubmitted(Container container,
                                      RoleInstance instance) {
    instance.state = STATE_SUBMITTED;
    instance.container = container;
    instance.createTime = now();
    getStartingContainers().put(container.getId(), instance);
    putOwnedContainer(container.getId(), instance);
    roleHistory.onContainerStartSubmitted(container, instance);
  }

  /**
   * Note that a container has been submitted for release; update internal state
   * and mark the associated ContainerInfo released field to indicate that
   * while it is still in the active list, it has been queued for release.
   *
   * @param container container
   * @throws SliderInternalStateException if there is no container of that ID
   * on the active list
   */
  public synchronized void containerReleaseSubmitted(Container container)
      throws SliderInternalStateException {
    ContainerId id = container.getId();
    //look up the container
    RoleInstance instance = getOwnedContainer(id);
    if (instance == null) {
      throw new SliderInternalStateException(
        "No active container with ID " + id);
    }
    //verify that it isn't already released
    if (containersBeingReleased.containsKey(id)) {
      throw new SliderInternalStateException(
        "Container %s already queued for release", id);
    }
    instance.released = true;
    containersBeingReleased.put(id, instance.container);
    roleHistory.onContainerReleaseSubmitted(container);
  }

  /**
   * Create a container request.
   * Update internal state, such as the role request count.
   * Anti-Affine: the {@link RoleStatus#outstandingAArequest} is set here.
   * This is where role history information will be used for placement decisions.
   * @param role role
   * @return the container request to submit or null if there is none
   */
  private AMRMClient.ContainerRequest createContainerRequest(RoleStatus role) {
    if (role.isAntiAffinePlacement()) {
      return createAAContainerRequest(role);
    } else {
      OutstandingRequest request = roleHistory.requestContainerForRole(role);
      if (request != null) {
        return request.getIssuedRequest();
      } else {
        return null;
      }
    }
  }

  /**
   * Create a container request.
   * Update internal state, such as the role request count.
   * Anti-Affine: the {@link RoleStatus#outstandingAArequest} is set here.
   * This is where role history information will be used for placement decisions.
   * @param role role
   * @return the container request to submit or null if there is none
   */
  private AMRMClient.ContainerRequest createAAContainerRequest(RoleStatus role) {
    OutstandingRequest request = roleHistory.requestContainerForAARole(role);
    if (request == null) {
      return null;
    }
    role.setOutstandingAArequest(request);
    return request.getIssuedRequest();
  }

  @VisibleForTesting
  public void incRequestedContainers(RoleStatus role) {
    log.info("Incrementing requested containers for {}", role.getName());
    role.getComponentMetrics().containersRequested.incr();
    appMetrics.containersRequested.incr();
  }

  private void decRequestedContainers(RoleStatus role) {
    role.getComponentMetrics().containersRequested.decr();
    appMetrics.containersRequested.decr();
    log.info("Decrementing requested containers for {} by {} to {}", role
        .getName(), 1, role.getComponentMetrics().containersRequested.value());
  }

  private int decRequestedContainersToFloor(RoleStatus role, int delta) {
    int actual = decMetricToFloor(role.getComponentMetrics()
        .containersRequested, delta);
    appMetrics.containersRequested.decr(actual);
    log.info("Decrementing requested containers for {} by {} to {}", role
            .getName(), actual, role.getComponentMetrics().containersRequested
        .value());
    return actual;
  }

  private int decAAPendingToFloor(RoleStatus role, int delta) {
    int actual = decMetricToFloor(role.getComponentMetrics()
        .pendingAAContainers, delta);
    appMetrics.pendingAAContainers.decr(actual);
    log.info("Decrementing AA pending containers for {} by {} to {}", role
        .getName(), actual, role.getComponentMetrics().pendingAAContainers
        .value());
    return actual;
  }

  private int decMetricToFloor(MutableGaugeInt metric, int delta) {
    int currentValue = metric.value();
    int decrAmount = delta;
    if (currentValue - delta < 0) {
      decrAmount = currentValue;
    }
    metric.decr(decrAmount);
    return decrAmount;
  }

  @VisibleForTesting
  public void incRunningContainers(RoleStatus role) {
    role.getComponentMetrics().containersRunning.incr();
    appMetrics.containersRunning.incr();
  }

  private void decRunningContainers(RoleStatus role) {
    role.getComponentMetrics().containersRunning.decr();
    appMetrics.containersRunning.decr();
  }

  private void setDesiredContainers(RoleStatus role, int n) {
    int delta = n - role.getComponentMetrics().containersDesired.value();
    role.getComponentMetrics().containersDesired.set(n);
    appMetrics.containersDesired.incr(delta);
  }

  private void incCompletedContainers(RoleStatus role) {
    role.getComponentMetrics().containersCompleted.incr();
    appMetrics.containersCompleted.incr();
  }

  @VisibleForTesting
  public void incFailedContainers(RoleStatus role, ContainerOutcome outcome) {
    switch (outcome) {
    case Preempted:
      appMetrics.containersPreempted.incr();
      role.getComponentMetrics().containersPreempted.incr();
      break;
    case Disk_failure:
      appMetrics.containersDiskFailure.incr();
      appMetrics.containersFailed.incr();
      role.getComponentMetrics().containersDiskFailure.incr();
      role.getComponentMetrics().containersFailed.incr();
      break;
    case Failed:
      appMetrics.failedSinceLastThreshold.incr();
      appMetrics.containersFailed.incr();
      role.getComponentMetrics().failedSinceLastThreshold.incr();
      role.getComponentMetrics().containersFailed.incr();
      break;
    case Failed_limits_exceeded:
      appMetrics.containersLimitsExceeded.incr();
      appMetrics.failedSinceLastThreshold.incr();
      appMetrics.containersFailed.incr();
      role.getComponentMetrics().containersLimitsExceeded.incr();
      role.getComponentMetrics().failedSinceLastThreshold.incr();
      role.getComponentMetrics().containersFailed.incr();
      break;
    default:
      appMetrics.failedSinceLastThreshold.incr();
      appMetrics.containersFailed.incr();
      role.getComponentMetrics().failedSinceLastThreshold.incr();
      role.getComponentMetrics().containersFailed.incr();
      break;
    }
  }

  /**
   * Build up the resource requirements for this role from the cluster
   * specification, including substituting max allowed values if the
   * specification asked for it (except when
   * {@link org.apache.slider.api.ResourceKeys#YARN_RESOURCE_NORMALIZATION_ENABLED}
   * is set to false).
   * @param role role
   * during normalization
   */
  public Resource buildResourceRequirements(RoleStatus role) {
    // Set up resource requirements from role values
    String name = role.getName();
    Component component = role.getProviderRole().component;
    int cores = DEF_YARN_CORES;
    if (component.getResource() != null && component.getResource().getCpus()
        != null) {
      cores = Math.min(containerMaxCores, component.getResource().getCpus());
    }
    if (cores <= 0) {
      cores = DEF_YARN_CORES;
    }
    long rawMem = DEF_YARN_MEMORY;
    if (component.getResource() != null && component.getResource().getMemory()
        != null) {
      if (YARN_RESOURCE_MAX.equals(component.getResource().getMemory())) {
        rawMem = containerMaxMemory;
      } else {
        rawMem = Long.parseLong(component.getResource().getMemory());
      }
    }
    boolean normalize = component.getConfiguration().getPropertyBool(
        YARN_RESOURCE_NORMALIZATION_ENABLED, true);
    if (!normalize) {
      log.info("Resource normalization: disabled");
      log.debug("Component {} has RAM={}, vCores={}", name, rawMem, cores);
      return Resources.createResource(rawMem, cores);
    }
    long mem = Math.min(containerMaxMemory, rawMem);
    if (mem <= 0) {
      mem = DEF_YARN_MEMORY;
    }
    Resource capability = Resource.newInstance(mem, cores);
    log.debug("Component {} has RAM={}, vCores={}", name, mem, cores);
    Resource normalized = recordFactory.normalize(capability, minResource,
        maxResource);
    if (!Resources.equals(normalized, capability)) {
      // resource requirements normalized to something other than asked for.
      // LOG @ WARN so users can see why this is happening.
      log.warn("Resource requirements of {} normalized" +
              " from {} to {}", name, capability, normalized);
    }
    return normalized;
  }

  /**
   * add a launched container to the node map for status responses
   * @param container id
   * @param node node details
   */
  private void addLaunchedContainer(Container container, RoleInstance node) {
    node.container = container;
    if (node.role == null) {
      throw new RuntimeException(
        "Unknown role for node " + node);
    }
    getLiveContainers().put(node.getContainerId(), node);
    //tell role history
    roleHistory.onContainerStarted(container);
  }

  /**
   * container start event
   * @param containerId container that is to be started
   * @return the role instance, or null if there was a problem
   */
  public synchronized RoleInstance onNodeManagerContainerStarted(ContainerId containerId) {
    try {
      return innerOnNodeManagerContainerStarted(containerId);
    } catch (YarnRuntimeException e) {
      log.error("NodeManager callback on started container {} failed",
                containerId,
                e);
      return null;
    }
  }

   /**
   * container start event handler -throwing an exception on problems
   * @param containerId container that is to be started
   * @return the role instance
   * @throws RuntimeException on problems
   */
  @VisibleForTesting
  public RoleInstance innerOnNodeManagerContainerStarted(ContainerId containerId) {
    RoleInstance instance = getOwnedContainer(containerId);
    if (instance == null) {
      //serious problem
      throw new YarnRuntimeException("Container not in active containers start "+
                containerId);
    }
    if (instance.role == null) {
      throw new YarnRuntimeException("Component instance has no instance name " +
                                     instance);
    }
    instance.startTime = now();
    RoleInstance starting = getStartingContainers().remove(containerId);
    if (null == starting) {
      throw new YarnRuntimeException(
        "Container "+ containerId +" is already started");
    }
    instance.state = STATE_LIVE;
    Container container = instance.container;
    addLaunchedContainer(container, instance);
    return instance;
  }

  /**
   * update the application state after a failure to start a container.
   * This is perhaps where blacklisting could be most useful: failure
   * to start a container is a sign of a more serious problem
   * than a later exit.
   *
   * -relayed from NMClientAsync.CallbackHandler 
   * @param containerId failing container
   * @param thrown what was thrown
   */
  public synchronized void onNodeManagerContainerStartFailed(ContainerId containerId,
                                                             Throwable thrown) {
    removeOwnedContainer(containerId);
    RoleInstance instance = getStartingContainers().remove(containerId);
    if (null != instance) {
      RoleStatus roleStatus = lookupRoleStatus(instance.roleId);
      String text;
      if (null != thrown) {
        text = SliderUtils.stringify(thrown);
      } else {
        text = "container start failure";
      }
      instance.diagnostics = text;
      roleStatus.noteFailed(text);
      getFailedContainers().put(containerId, instance);
      roleHistory.onNodeManagerContainerStartFailed(instance.container);
      incFailedContainers(roleStatus, ContainerOutcome.Failed);
    }
  }

  /**
   * Handle node update from the RM. This syncs up the node map with the RM's view
   * @param updatedNodes updated nodes
   */
  public synchronized NodeUpdatedOutcome onNodesUpdated(List<NodeReport> updatedNodes) {
    boolean changed = roleHistory.onNodesUpdated(updatedNodes);
    if (changed) {
      log.info("YARN cluster changed —cancelling current AA requests");
      List<AbstractRMOperation> operations = cancelOutstandingAARequests();
      log.debug("Created {} cancel requests", operations.size());
      return new NodeUpdatedOutcome(true, operations);
    }
    return new NodeUpdatedOutcome(false, new ArrayList<>(0));
  }

  /**
   * Return value of the {@link #onNodesUpdated(List)} call.
   */
  public static class NodeUpdatedOutcome {
    public final boolean clusterChanged;
    public final List<AbstractRMOperation> operations;

    public NodeUpdatedOutcome(boolean clusterChanged,
        List<AbstractRMOperation> operations) {
      this.clusterChanged = clusterChanged;
      this.operations = operations;
    }
  }
  /**
   * Is a role short lived by the threshold set for this application
   * @param instance instance
   * @return true if the instance is considered short lived
   */
  @VisibleForTesting
  public boolean isShortLived(RoleInstance instance) {
    long time = now();
    long started = instance.startTime;
    boolean shortlived;
    if (started > 0) {
      long duration = time - started;
      shortlived = duration < (startTimeThreshold * 1000);
      log.info("Duration {} and startTimeThreshold {}", duration, startTimeThreshold);
    } else {
      // never even saw a start event
      shortlived = true;
    }
    return shortlived;
  }

  /**
   * Current time in milliseconds. Made protected for
   * the option to override it in tests.
   * @return the current time.
   */
  protected long now() {
    return System.currentTimeMillis();
  }

  /**
   * This is a very small class to send a multiple result back from 
   * the completion operation
   */
  public static class NodeCompletionResult {
    public boolean surplusNode = false;
    public RoleInstance roleInstance;
    // did the container fail for *any* reason?
    public boolean containerFailed = false;
    // detailed outcome on the container failure
    public ContainerOutcome outcome = ContainerOutcome.Completed;
    public int exitStatus = 0;
    public boolean unknownNode = false;

    public String toString() {
      final StringBuilder sb =
        new StringBuilder("NodeCompletionResult{");
      sb.append("surplusNode=").append(surplusNode);
      sb.append(", roleInstance=").append(roleInstance);
      sb.append(", exitStatus=").append(exitStatus);
      sb.append(", containerFailed=").append(containerFailed);
      sb.append(", outcome=").append(outcome);
      sb.append(", unknownNode=").append(unknownNode);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * handle completed node in the CD -move something from the live
   * server list to the completed server list.
   * @param status the node that has just completed
   * @return NodeCompletionResult
   */
  public synchronized NodeCompletionResult onCompletedContainer(
      ContainerStatus status) {
    ContainerId containerId = status.getContainerId();
    NodeCompletionResult result = new NodeCompletionResult();
    RoleInstance roleInstance;

    int exitStatus = status.getExitStatus();
    result.exitStatus = exitStatus;
    if (containersBeingReleased.containsKey(containerId)) {
      log.info("Container was queued for release : {}", containerId);
      Container container = containersBeingReleased.remove(containerId);
      RoleStatus roleStatus = lookupRoleStatus(container);
      decRunningContainers(roleStatus);
      incCompletedContainers(roleStatus);
      log.info("decrementing role count for role {} to {}; completed={}",
          roleStatus.getName(),
          roleStatus.getComponentMetrics().containersRunning.value(),
          roleStatus.getComponentMetrics().containersCompleted.value());
      result.outcome = ContainerOutcome.Completed;
      roleHistory.onReleaseCompleted(container);

    } else if (surplusContainers.remove(containerId)) {
      //its a surplus one being purged
      result.surplusNode = true;
    } else {
      // a container has failed or been killed
      // use the exit code to determine the outcome
      result.containerFailed = true;
      result.outcome = ContainerOutcome.fromExitStatus(exitStatus);

      roleInstance = removeOwnedContainer(containerId);
      if (roleInstance != null) {
        RoleStatus roleStatus = lookupRoleStatus(roleInstance.roleId);
        incFailedContainers(roleStatus, result.outcome);
        failedContainers.put(containerId, roleInstance);
      } else {
        // the container may have been noted as failed already, so look
        // it up
        roleInstance = failedContainers.get(containerId);
      }
      if (roleInstance != null) {
        int roleId = roleInstance.roleId;
        String rolename = roleInstance.role;
        log.info("Failed container in role[{}] : {}", roleId,
            roleInstance.getCompInstanceName());
        try {
          RoleStatus roleStatus = lookupRoleStatus(roleInstance.roleId);
          decRunningContainers(roleStatus);
          roleStatus.getProviderRole().failedInstances.offer(roleInstance);
          boolean shortLived = isShortLived(roleInstance);
          String message;
          Container failedContainer = roleInstance.container;

          //build the failure message
          if (failedContainer != null) {
            String completedLogsUrl = getLogsURLForContainer(failedContainer);
            message = String.format("Failure %s on host %s (%d): %s",
                roleInstance.getContainerId(),
                failedContainer.getNodeId().getHost(),
                exitStatus,
                completedLogsUrl);
          } else {
            message = String.format("Failure %s (%d)", containerId, exitStatus);
          }
          roleStatus.noteFailed(message);
          long failed =
              roleStatus.getComponentMetrics().containersFailed.value();
          log.info("Current count of failed role[{}] {} =  {}",
              roleId, rolename, failed);
          if (failedContainer != null) {
            roleHistory.onFailedContainer(failedContainer, shortLived, result.outcome);
          }

        } catch (YarnRuntimeException e1) {
          log.error("Failed container of unknown role {}", roleId);
        }
      } else {
        //this isn't a known container.

        log.error("Notified of completed container {} that is not in the list" +
            " of active or failed containers", containerId);
        completionOfUnknownContainerEvent.incrementAndGet();
        result.unknownNode = true;
      }
    }

    if (result.surplusNode) {
      //a surplus node
      return result;
    }

    //record the complete node's details; this pulls it from the livenode set 
    //remove the node
    ContainerId id = status.getContainerId();
    log.info("Removing node ID {}", id);
    RoleInstance node = getLiveContainers().remove(id);
    if (node != null) {
      node.state = STATE_DESTROYED;
      node.exitCode = exitStatus;
      node.diagnostics = status.getDiagnostics();
      getCompletedContainers().put(id, node);
      result.roleInstance = node;
    } else {
      // not in the list
      log.warn("Received notification of completion of unknown node {}", id);
      completionOfNodeNotInLiveListEvent.incrementAndGet();
    }

    // and the active node list if present
    removeOwnedContainer(containerId);

    // finally, verify the node doesn't exist any more
    assert !containersBeingReleased.containsKey(
        containerId) : "container still in release queue";
    assert !getLiveContainers().containsKey(
        containerId) : " container still in live nodes";
    assert getOwnedContainer(containerId) ==
           null : "Container still in active container list";

    return result;
  }

  /**
   * Get the URL log for a container
   * @param c container
   * @return the URL or "" if it cannot be determined
   */
  protected String getLogsURLForContainer(Container c) {
    if (c==null) {
      return null;
    }
    String user = null;
    try {
      user = SliderUtils.getCurrentUser().getShortUserName();
    } catch (IOException ignored) {
    }
    String completedLogsUrl = "";
    String url = logServerURL;
    if (user != null && SliderUtils.isSet(url)) {
      completedLogsUrl = url
          + "/" + c.getNodeId() + "/" + c.getId() + "/ctx/" + user;
    }
    return completedLogsUrl;
  }

  /**
   * Return the percentage done that Slider is to have YARN display in its
   * Web UI
   * @return an number from 0 to 100
   */
  public synchronized float getApplicationProgressPercentage() {
    float percentage;
    long desired = 0;
    float actual = 0;
    for (RoleStatus role : getRoleStatusMap().values()) {
      desired += role.getDesired();
      actual += role.getRunning();
    }
    if (desired == 0) {
      percentage = 100;
    } else {
      percentage = actual / desired;
    }
    return percentage;
  }


  /**
   * Update the cluster description with the current application state
   */

  public synchronized Application refreshClusterStatus() {

    //TODO replace ClusterDescription with Application + related statistics
    //TODO build container stats
    app.setState(ApplicationState.STARTED);
    return app;
  }

  /**
   * get application liveness information
   * @return a snapshot of the current liveness information
   */  
  public ApplicationLivenessInformation getApplicationLivenessInformation() {
    ApplicationLivenessInformation li = new ApplicationLivenessInformation();
    RoleStatistics stats = getRoleStatistics();
    int outstanding = (int)(stats.desired - stats.actual);
    li.requestsOutstanding = outstanding;
    li.allRequestsSatisfied = outstanding <= 0;
    return li;
  }


  /**
   * Get the aggregate statistics across all roles
   * @return role statistics
   */
  public RoleStatistics getRoleStatistics() {
    RoleStatistics stats = new RoleStatistics();
    for (RoleStatus role : getRoleStatusMap().values()) {
      stats.add(role.getStatistics());
    }
    return stats;
  }

  /**
   * Get a snapshot of component information.
   * <p>
   *   This does <i>not</i> include any container list, which 
   *   is more expensive to create.
   * @return a map of current role status values.
   */
  public Map<String, ComponentInformation> getComponentInfoSnapshot() {

    Map<Integer, RoleStatus> statusMap = getRoleStatusMap();
    Map<String, ComponentInformation> results = new HashMap<>(
            statusMap.size());

    for (RoleStatus status : statusMap.values()) {
      String name = status.getName();
      ComponentInformation info = status.serialize();
      results.put(name, info);
    }
    return results;
  }

  public synchronized AbstractRMOperation updateBlacklist() {
    UpdateBlacklistOperation blacklistOperation =
        roleHistory.updateBlacklist(getRoleStatusMap().values());
    if (blacklistOperation != null) {
      log.info("Updating {}", blacklistOperation);
    }
    return blacklistOperation;
  }

  /**
   * Look at where the current node state is -and whether it should be changed
   */
  public synchronized List<AbstractRMOperation> reviewRequestAndReleaseNodes()
      throws SliderInternalStateException, TriggerClusterTeardownException {
    log.info("in reviewRequestAndReleaseNodes()");
    List<AbstractRMOperation> allOperations = new ArrayList<>();
    AbstractRMOperation blacklistOperation = updateBlacklist();
    if (blacklistOperation != null) {
      allOperations.add(blacklistOperation);
    }
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      if (!roleStatus.isExcludeFromFlexing() &&
          areDependenciesReady(roleStatus)) {
        List<AbstractRMOperation> operations = reviewOneRole(roleStatus);
        allOperations.addAll(operations);
      }
    }
    return allOperations;
  }

  @VisibleForTesting
  public boolean areDependenciesReady(RoleStatus roleStatus) {
    List<String> dependencies = roleStatus.getProviderRole().component
        .getDependencies();
    if (SliderUtils.isEmpty(dependencies)) {
      return true;
    }
    for (String dependency : dependencies) {
      ProviderRole providerRole = roles.get(dependency);
      if (providerRole == null) {
        log.error("Couldn't find dependency {} for {} (should never happen)",
            dependency, roleStatus.getName());
        continue;
      }
      RoleStatus other = getRoleStatusMap().get(providerRole.id);
      if (other.getRunning() < other.getDesired()) {
        log.info("Dependency {} not satisfied for {}, only {} of {} instances" +
            " running", dependency, roleStatus.getName(), other.getRunning(),
            other.getDesired());
        return false;
      }
      if (providerRole.probe == null) {
        continue;
      }
      List<RoleInstance> dependencyInstances = enumLiveNodesInRole(
          providerRole.name);
      if (dependencyInstances.size() < other.getDesired()) {
        log.info("Dependency {} not satisfied for {}, only {} of {} instances" +
                " live", dependency, roleStatus.getName(),
            dependencyInstances.size(), other.getDesired());
        return false;
      }
      for (RoleInstance instance : dependencyInstances) {
        if (instance.state != STATE_READY) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Check the "recent" failure threshold for a role
   * @param role role to examine
   * @throws TriggerClusterTeardownException if the role
   * has failed too many times
   */
  private void checkFailureThreshold(RoleStatus role)
      throws TriggerClusterTeardownException {
    long failures = role.getFailedRecently();
    int threshold = getFailureThresholdForRole(role);
    if (log.isDebugEnabled() && failures > 0) {
      log.debug("Failure count of component: {}: {}, threshold={}",
          role.getName(), failures, threshold);
    }

    if (threshold > 0 && failures > threshold) {
      throw new TriggerClusterTeardownException(
          SliderExitCodes.EXIT_DEPLOYMENT_FAILED, FinalApplicationStatus.FAILED,
          ErrorStrings.E_UNSTABLE_CLUSTER
              + " - failed with component %s failed 'recently' %d times;"
              + " threshold is %d - last failure: %s", role.getName(),
          role.getFailedRecently(), threshold, role.getFailureMessage());
    }
  }

  /**
   * Get the failure threshold for a specific role, falling back to
   * the global one if not
   * @param roleStatus role
   * @return the threshold for failures
   */
  private int getFailureThresholdForRole(RoleStatus roleStatus) {
    return (int) roleStatus.getProviderRole().component.getConfiguration()
        .getPropertyLong(CONTAINER_FAILURE_THRESHOLD,
            failureThreshold);
  }


  /**
   * Reset the "recent" failure counts of all roles
   */
  public void resetFailureCounts() {
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      long failed = roleStatus.resetFailedRecently();
      log.info("Resetting failure count of {}; was {}", roleStatus.getName(),
          failed);

    }
    roleHistory.resetFailedRecently();
  }

  /**
   * Escalate operation as triggered by external timer.
   * @return a (usually empty) list of cancel/request operations.
   */
  public List<AbstractRMOperation> escalateOutstandingRequests() {
    return roleHistory.escalateOutstandingRequests();
  }

  /**
   * Cancel any outstanding AA Requests, building up the list of ops to
   * cancel, removing them from RoleHistory structures and the RoleStatus
   * entries.
   * @return a (usually empty) list of cancel/request operations.
   */
  public synchronized List<AbstractRMOperation> cancelOutstandingAARequests() {
    // get the list of cancel operations
    List<AbstractRMOperation> operations = roleHistory.cancelOutstandingAARequests();
    for (RoleStatus roleStatus : roleStatusMap.values()) {
      if (roleStatus.isAARequestOutstanding()) {
        log.info("Cancelling outstanding AA request for {}", roleStatus);
        roleStatus.cancelOutstandingAARequest();
      }
    }
    return operations;
  }

  public synchronized boolean monitorComponentInstances() {
    boolean hasChanged = false;
    for (RoleInstance instance : getLiveContainers().values()) {
      if (instance.providerRole.probe == null) {
        continue;
      }
      boolean ready = instance.providerRole.probe.ping(instance).isSuccess();
      if (ready) {
        if (instance.state != STATE_READY) {
          instance.state = STATE_READY;
          hasChanged = true;
          log.info("State of {} changed to ready", instance.role);
        }
      } else {
        if (instance.state == STATE_READY) {
          instance.state = STATE_NOT_READY;
          hasChanged = true;
          log.info("State of {} changed from ready to not ready", instance
              .role);
        }
      }
    }
    return hasChanged;
  }

  /**
   * Look at the allocation status of one role, and trigger add/release
   * actions if the number of desired role instances doesn't equal
   * (actual + pending).
   * <p>
   * MUST be executed from within a synchronized method
   * <p>
   * @param role role
   * @return a list of operations
   * @throws SliderInternalStateException if the operation reveals that
   * the internal state of the application is inconsistent.
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private List<AbstractRMOperation> reviewOneRole(RoleStatus role)
      throws SliderInternalStateException, TriggerClusterTeardownException {
    List<AbstractRMOperation> operations = new ArrayList<>();
    long delta;
    long expected;
    String name = role.getName();
    synchronized (role) {
      delta = role.getDelta();
      expected = role.getDesired();
    }

    log.info("Reviewing " + role.getName() + ": " + role.getComponentMetrics());
    checkFailureThreshold(role);

    if (expected < 0 ) {
      // negative value: fail
      throw new TriggerClusterTeardownException(
          SliderExitCodes.EXIT_DEPLOYMENT_FAILED,
          FinalApplicationStatus.FAILED,
          "Negative component count of %d desired for component %s",
          expected, role);
    }

    if (delta > 0) {
      // more workers needed than we have -ask for more
      log.info("{}: Asking for {} more nodes(s) for a total of {} ", name, delta, expected);

      if (role.isAntiAffinePlacement()) {
        long pending = delta;
        if (roleHistory.canPlaceAANodes()) {
          // build one only if there is none outstanding, the role history knows
          // enough about the cluster to ask, and there is somewhere to place
          // the node
          if (!role.isAARequestOutstanding()) {
            // no outstanding AA; try to place things
            AMRMClient.ContainerRequest request = createAAContainerRequest(role);
            if (request != null) {
              pending--;
              log.info("Starting an anti-affine request sequence for {} nodes; pending={}",
                delta, pending);
              addContainerRequest(operations, request, role);
            } else {
              log.info("No location for anti-affine request");
            }
          }
        } else {
          log.warn("Awaiting node map before generating anti-affinity requests");
        }
        log.info("Setting pending to {}", pending);
        //TODO
        role.setAAPending(pending);
      } else {

        for (int i = 0; i < delta; i++) {
          //get the role history to select a suitable node, if available
          addContainerRequest(operations, createContainerRequest(role), role);
        }
      }
    } else if (delta < 0) {
      log.info("{}: Asking for {} fewer node(s) for a total of {}", name,
               -delta,
               expected);
      // reduce the number expected (i.e. subtract the delta)
      long excess = -delta;

      // how many requests are outstanding? for AA roles, this includes pending
      long outstandingRequests = role.getRequested() + role.getAAPending();
      if (outstandingRequests > 0) {
        // outstanding requests.
        int toCancel = (int)Math.min(outstandingRequests, excess);

        int pendingCancelled = 0;
        if (role.getAAPending() > 0) {
          pendingCancelled = decAAPendingToFloor(role, toCancel);
        }
        int remainingToCancel = toCancel - pendingCancelled;

        // Delegate to Role History
        List<AbstractRMOperation> cancellations = roleHistory
            .cancelRequestsForRole(role, remainingToCancel);
        log.info("Found {} outstanding requests to cancel", cancellations.size());
        operations.addAll(cancellations);
        if (remainingToCancel != cancellations.size()) {
          log.error("Tracking of outstanding requests is not in sync with the summary statistics:" +
              " expected to be able to cancel {} requests, but got {}",
              remainingToCancel, cancellations.size());
        }

        int requestCancelled = decRequestedContainersToFloor(role,
            remainingToCancel);
        excess -= pendingCancelled;
        excess -= requestCancelled;
        assert excess >= 0 : "Attempted to cancel too many requests";
        log.info("Submitted {} cancellations, leaving {} to release",
            pendingCancelled + requestCancelled, excess);
        if (excess == 0) {
          log.info("After cancelling requests, application is now at desired size");
        }
      }

      // after the cancellation there may be no excess
      if (excess > 0) {

        // there's an excess, so more to cancel
        // get the nodes to release
        int roleId = role.getKey();

        // enum all active nodes that aren't being released
        List<RoleInstance> containersToRelease = enumNodesWithRoleId(roleId, true);
        if (containersToRelease.isEmpty()) {
          log.info("No containers for component {}", roleId);
        }

        // filter out all release-in-progress nodes
        ListIterator<RoleInstance> li = containersToRelease.listIterator();
        while (li.hasNext()) {
          RoleInstance next = li.next();
          if (next.released) {
            li.remove();
          }
        }

        // warn if the desired state can't be reached
        int numberAvailableForRelease = containersToRelease.size();
        if (numberAvailableForRelease < excess) {
          log.warn("Not enough containers to release, have {} and need {} more",
              numberAvailableForRelease,
              excess - numberAvailableForRelease);
        }

        // ask the release selector to sort the targets
        containersToRelease =  containerReleaseSelector.sortCandidates(
            roleId,
            containersToRelease);

        // crop to the excess
        List<RoleInstance> finalCandidates = (excess < numberAvailableForRelease)
            ? containersToRelease.subList(0, (int)excess)
            : containersToRelease;

        // then build up a release operation, logging each container as released
        for (RoleInstance possible : finalCandidates) {
          log.info("Targeting for release: {}", possible);
          containerReleaseSubmitted(possible.container);
          role.getProviderRole().failedInstances.offer(possible);
          operations.add(new ContainerReleaseOperation(possible.getContainerId()));
        }
      }

    } else {
      // actual + requested == desired
      // there's a special case here: clear all pending AA requests
      if (role.getAAPending() > 0) {
        log.debug("Clearing outstanding pending AA requests");
        role.setAAPending(0);
      }
    }

    // there's now a list of operations to execute
    log.debug("operations scheduled: {}; updated role: {}", operations.size(), role);
    return operations;
  }

  /**
   * Add a container request if the request is non-null
   * @param operations operations to add the entry to
   * @param containerAsk what to ask for
   * @return true if a request was added
   */
  private boolean addContainerRequest(List<AbstractRMOperation> operations,
      AMRMClient.ContainerRequest containerAsk, RoleStatus role) {
    if (containerAsk != null) {
      log.info("Container ask is {} and label = {}", containerAsk,
          containerAsk.getNodeLabelExpression());
      int askMemory = containerAsk.getCapability().getMemory();
      if (askMemory > this.containerMaxMemory) {
        log.warn("Memory requested: {} > max of {}", askMemory, containerMaxMemory);
      }
      operations.add(new ContainerRequestOperation(containerAsk));
      incRequestedContainers(role);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Releases a container based on container id
   * @param containerId
   * @return
   * @throws SliderInternalStateException
   */
  public List<AbstractRMOperation> releaseContainer(ContainerId containerId)
      throws SliderInternalStateException {
    List<AbstractRMOperation> operations = new ArrayList<AbstractRMOperation>();
    List<RoleInstance> activeRoleInstances = cloneOwnedContainerList();
    for (RoleInstance role : activeRoleInstances) {
      if (role.container.getId().equals(containerId)) {
        containerReleaseSubmitted(role.container);
        operations.add(new ContainerReleaseOperation(role.getContainerId()));
      }
    }

    return operations;
  }

  /**
   * Release all containers.
   * @return a list of operations to execute
   */
  public synchronized List<AbstractRMOperation> releaseAllContainers() {

    Collection<RoleInstance> targets = cloneOwnedContainerList();
    log.info("Releasing {} containers", targets.size());
    List<AbstractRMOperation> operations =
      new ArrayList<>(targets.size());
    for (RoleInstance instance : targets) {
      if (instance.roleId == SliderKeys.ROLE_AM_PRIORITY_INDEX) {
        // don't worry about the AM
        continue;
      }
      Container possible = instance.container;
      ContainerId id = possible.getId();
      if (!instance.released) {
        String url = getLogsURLForContainer(possible);
        log.info("Releasing container. Log: " + url);
        try {
          containerReleaseSubmitted(possible);
          // update during finish call
          if (serviceTimelinePublisher != null) {
            serviceTimelinePublisher.componentInstanceFinished(instance);
          }
        } catch (SliderInternalStateException e) {
          log.warn("when releasing container {} :", possible, e);
        }
        operations.add(new ContainerReleaseOperation(id));
      }
    }
    return operations;
  }

  /**
   * Event handler for allocated containers: builds up the lists
   * of assignment actions (what to run where), and possibly
   * a list of operations to perform
   * @param allocatedContainers the containers allocated
   * @param assignments the assignments of roles to containers
   * @param operations any allocation or release operations
   */
  public synchronized void onContainersAllocated(
      List<Container> allocatedContainers,
      List<ContainerAssignment> assignments,
      List<AbstractRMOperation> operations) {
    assignments.clear();
    operations.clear();
    List<Container> ordered = roleHistory.prepareAllocationList(allocatedContainers);
    log.info("onContainersAllocated(): Total containers allocated = {}", ordered.size());
    for (Container container : ordered) {
      final NodeId nodeId = container.getNodeId();
      String containerHostInfo = nodeId.getHost() + ":" + nodeId.getPort();
      //get the role
      final ContainerId cid = container.getId();
      final RoleStatus role = lookupRoleStatus(container);

      //inc allocated count -this may need to be dropped in a moment,
      // but us needed to update the logic below
      MutableGaugeInt containersRunning = role.getComponentMetrics().containersRunning;
      incRunningContainers(role);
      final long allocated = containersRunning.value();
      final long desired = role.getDesired();

      final String roleName = role.getName();
      final ContainerAllocationResults allocation =
          roleHistory.onContainerAllocated(container, desired, allocated);
      final ContainerAllocationOutcome outcome = allocation.outcome;

      // add all requests to the operations list
      operations.addAll(allocation.operations);

      //look for condition where we get more back than we asked
      if (allocated > desired) {
        log.info("Discarding surplus {} container {} on {}", roleName,  cid, containerHostInfo);
        operations.add(new ContainerReleaseOperation(cid));
        //register as a surplus node
        surplusContainers.add(cid);
        role.getComponentMetrics().surplusContainers.incr();
        containersRunning.decr();
      } else {
        decRequestedContainers(role);
        log.info("Assigning role {} to container" + " {}," + " on {}:{},",
            roleName, cid, nodeId.getHost(), nodeId.getPort());

        assignments.add(new ContainerAssignment(container, role, outcome));
        //add to the history
        roleHistory.onContainerAssigned(container);
        // now for AA requests, add some more
        if (role.isAntiAffinePlacement()) {
          role.completeOutstandingAARequest();
          // check invariants. The new node must become unavailable.
          NodeInstance node = roleHistory.getOrCreateNodeInstance(container);
          if (node.canHost(role.getKey(), role.getLabelExpression())) {
            log.error("Assigned node still declares as available {}", node.toFullString() );
          }
          if (role.getAAPending() > 0) {
            // still an outstanding AA request: need to issue a new one.
            log.info("Asking for next container for AA role {}", roleName);
            if (!addContainerRequest(operations, createAAContainerRequest(role),
                role)) {
              log.info("No capacity in cluster for new requests");
            } else {
              role.decAAPending();
            }
            log.debug("Current AA role status {}", role);
          } else {
            log.info("AA request sequence completed for role {}", role);
          }
        }

      }
    }
  }

  /**
   * Event handler for the list of active containers on restart.
   * Sets the info key {@link StatusKeys#INFO_CONTAINERS_AM_RESTART}
   * to the size of the list passed down (and does not set it if none were)
   * @param liveContainers the containers allocated
   * @return true if a rebuild took place (even if size 0)
   * @throws RuntimeException on problems
   */
  private boolean rebuildModelFromRestart(List<Container> liveContainers)
      throws BadClusterStateException {
    if (liveContainers == null) {
      return false;
    }
    for (Container container : liveContainers) {
      addRestartedContainer(container);
    }
    app.setNumberOfRunningContainers((long)liveContainers.size());
    return true;
  }

  /**
   * Add a restarted container by walking it through the create/submit/start
   * lifecycle, so building up the internal structures
   * @param container container that was running before the AM restarted
   * @throws RuntimeException on problems
   */
  private void addRestartedContainer(Container container)
      throws BadClusterStateException {
    String containerHostInfo = container.getNodeId().getHost()
                               + ":" +
                               container.getNodeId().getPort();
    // get the container ID
    ContainerId cid = container.getId();
    
    // get the role
    int roleId = ContainerPriority.extractRole(container);
    RoleStatus role = lookupRoleStatus(roleId);
    // increment its count
    incRunningContainers(role);
    String roleName = role.getName();
    
    log.info("Rebuilding container {} in role {} on {},",
             cid,
             roleName,
             containerHostInfo);

    //update app state internal structures and maps

    //TODO recover the component instance name from zk registry ?
    RoleInstance instance = new RoleInstance(container);
    instance.command = roleName;
    instance.role = roleName;
    instance.roleId = roleId;
    instance.environment = new String[0];
    instance.container = container;
    instance.createTime = now();
    instance.state = STATE_LIVE;
    instance.appVersion = SliderKeys.APP_VERSION_UNKNOWN;
    putOwnedContainer(cid, instance);
    //role history gets told
    roleHistory.onContainerAssigned(container);
    // pretend the container has just had its start actions submitted
    containerStartSubmitted(container, instance);
    // now pretend it has just started
    innerOnNodeManagerContainerStarted(cid);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("AppState{");
    sb.append("applicationLive=").append(applicationLive);
    sb.append(", live nodes=").append(liveNodes.size());
    sb.append('}');
    return sb.toString();
  }

  /**
   * Build map of role ID-> name
   * @return
   */
  public Map<Integer, String> buildNamingMap() {
    Map<Integer, RoleStatus> statusMap = getRoleStatusMap();
    Map<Integer, String> naming = new HashMap<>(statusMap.size());
    for (Map.Entry<Integer, RoleStatus> entry : statusMap.entrySet()) {
      naming.put(entry.getKey(), entry.getValue().getName());
    }
    return naming;
  }

  public void setServiceTimelinePublisher(ServiceTimelinePublisher serviceTimelinePublisher) {
    this.serviceTimelinePublisher = serviceTimelinePublisher;
  }
}
