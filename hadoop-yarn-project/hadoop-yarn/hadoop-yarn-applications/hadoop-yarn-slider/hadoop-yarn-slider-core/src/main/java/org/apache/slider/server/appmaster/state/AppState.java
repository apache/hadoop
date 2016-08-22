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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterDescriptionKeys;
import org.apache.slider.api.ClusterDescriptionOperations;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.RoleStatistics;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.exceptions.SliderInternalStateException;
import org.apache.slider.core.exceptions.TriggerClusterTeardownException;
import org.apache.slider.core.persist.AggregateConfSerDeser;
import org.apache.slider.core.persist.ConfTreeSerDeser;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.appmaster.management.LongGauge;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.management.MetricsConstants;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.slider.api.ResourceKeys.*;
import static org.apache.slider.api.RoleKeys.*;
import static org.apache.slider.api.StateValues.*;

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

  /**
   * The definition of the instance. Flexing updates the resources section
   * This is used as a synchronization point on activities that update
   * the CD, and also to update some of the structures that
   * feed in to the CD
   */
  private AggregateConf instanceDefinition;

  /**
   * Time the instance definition snapshots were created
   */
  private long snapshotTime;

  /**
   * Snapshot of the instance definition. This is fully
   * resolved.
   */
  private AggregateConf instanceDefinitionSnapshot;

  /**
   * Snapshot of the raw instance definition; unresolved and
   * without any patch of an AM into it.
   */
  private AggregateConf unresolvedInstanceDefinition;

  /**
   * snapshot of resources as of last update time
   */
  private ConfTreeOperations resourcesSnapshot;
  private ConfTreeOperations appConfSnapshot;
  private ConfTreeOperations internalsSnapshot;

  /**
   * This is the status, the live model
   */
  private ClusterDescription clusterStatus = new ClusterDescription();

  /**
   * Metadata provided by the AM for use in filling in status requests
   */
  private Map<String, String> applicationInfo;

  /**
   * Client properties created via the provider -static for the life
   * of the application
   */
  private Map<String, String> clientProperties = new HashMap<>();

  /**
   * This is a template of the cluster status
   */
  private ClusterDescription clusterStatusTemplate = new ClusterDescription();

  private final Map<Integer, RoleStatus> roleStatusMap =
    new ConcurrentSkipListMap<>();

  private final Map<String, ProviderRole> roles =
    new ConcurrentHashMap<>();

  private final ConcurrentSkipListMap<Integer, ProviderRole> rolePriorityMap =
    new ConcurrentSkipListMap<>();

  /**
   * The master node.
   */
  private RoleInstance appMasterNode;

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
   * Counter for completed containers ( complete denotes successful or failed )
   */
  private final LongGauge completedContainerCount = new LongGauge();

  /**
   *   Count of failed containers
   */
  private final LongGauge failedContainerCount = new LongGauge();

  /**
   * # of started containers
   */
  private final LongGauge startedContainers = new LongGauge();

  /**
   * # of containers that failed to start 
   */
  private final LongGauge startFailedContainerCount = new LongGauge();

  /**
   * Track the number of surplus containers received and discarded
   */
  private final LongGauge surplusContainers = new LongGauge();

  /**
   * Track the number of requested containers.
   * Important: this does not include AA requests which are yet to be issued.
   */
  private final LongGauge outstandingContainerRequests = new LongGauge();

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
  private final Set<ContainerId> surplusNodes = new HashSet<>();

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
  private Configuration publishedProviderConf;
  private long startTimeThreshold;

  private int failureThreshold = 10;
  private int nodeFailureThreshold = 3;

  private String logServerURL = "";

  /**
   * Selector of containers to release; application wide.
   */
  private ContainerReleaseSelector containerReleaseSelector;
  private Resource minResource;
  private Resource maxResource;

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

    // register any metrics
    register(MetricsConstants.CONTAINERS_OUTSTANDING_REQUESTS, outstandingContainerRequests);
    register(MetricsConstants.CONTAINERS_SURPLUS, surplusContainers);
    register(MetricsConstants.CONTAINERS_STARTED, startedContainers);
    register(MetricsConstants.CONTAINERS_COMPLETED, completedContainerCount);
    register(MetricsConstants.CONTAINERS_FAILED, failedContainerCount);
    register(MetricsConstants.CONTAINERS_START_FAILED, startFailedContainerCount);
  }

  private void register(String name, Metric counter) {
    this.metricsAndMonitoring.getMetrics().register(
        MetricRegistry.name(AppState.class, name), counter);
  }

  public long getFailedCountainerCount() {
    return failedContainerCount.getCount();
  }

  /**
   * Increment the count
   */
  public void incFailedCountainerCount() {
    failedContainerCount.inc();
  }

  public long getStartFailedCountainerCount() {
    return startFailedContainerCount.getCount();
  }

  /**
   * Increment the count and return the new value
   */
  public void incStartedCountainerCount() {
    startedContainers.inc();
  }

  public long getStartedCountainerCount() {
    return startedContainers.getCount();
  }

  /**
   * Increment the count and return the new value
   */
  public void incStartFailedCountainerCount() {
    startFailedContainerCount.inc();
  }

  public AtomicInteger getCompletionOfNodeNotInLiveListEvent() {
    return completionOfNodeNotInLiveListEvent;
  }

  public AtomicInteger getCompletionOfUnknownContainerEvent() {
    return completionOfUnknownContainerEvent;
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
   * <p>
   *   Calls to {@link #refreshClusterStatus()} trigger a
   *   refresh of this field.
   * <p>
   * This is read-only
   * to the extent that changes here do not trigger updates in the
   * application state. 
   * @return the cluster status
   */
  public synchronized ClusterDescription getClusterStatus() {
    return clusterStatus;
  }

  @VisibleForTesting
  protected synchronized void setClusterStatus(ClusterDescription clusterDesc) {
    this.clusterStatus = clusterDesc;
  }

  /**
   * Set the instance definition -this also builds the (now obsolete)
   * cluster specification from it.
   * 
   * Important: this is for early binding and must not be used after the build
   * operation is complete. 
   * @param definition initial definition
   * @throws BadConfigException
   */
  public synchronized void setInitialInstanceDefinition(AggregateConf definition)
      throws BadConfigException, IOException {
    log.debug("Setting initial instance definition");
    // snapshot the definition
    AggregateConfSerDeser serDeser = new AggregateConfSerDeser();

    unresolvedInstanceDefinition = serDeser.fromInstance(definition);
    
    this.instanceDefinition = serDeser.fromInstance(definition);
    onInstanceDefinitionUpdated();
  }

  public synchronized AggregateConf getInstanceDefinition() {
    return instanceDefinition;
  }

  /**
   * Get the role history of the application
   * @return the role history
   */
  @VisibleForTesting
  public RoleHistory getRoleHistory() {
    return roleHistory;
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

  public ConfTreeOperations getResourcesSnapshot() {
    return resourcesSnapshot;
  }

  public ConfTreeOperations getAppConfSnapshot() {
    return appConfSnapshot;
  }

  public ConfTreeOperations getInternalsSnapshot() {
    return internalsSnapshot;
  }

  public boolean isApplicationLive() {
    return applicationLive;
  }

  public long getSnapshotTime() {
    return snapshotTime;
  }

  public synchronized AggregateConf getInstanceDefinitionSnapshot() {
    return instanceDefinitionSnapshot;
  }

  public AggregateConf getUnresolvedInstanceDefinition() {
    return unresolvedInstanceDefinition;
  }

  public synchronized void buildInstance(AppStateBindingInfo binding)
      throws BadClusterStateException, BadConfigException, IOException {
    binding.validate();

    log.debug("Building application state");
    publishedProviderConf = binding.publishedProviderConf;
    applicationInfo = binding.applicationInfo != null ? binding.applicationInfo
                        : new HashMap<String, String>();

    clientProperties = new HashMap<>();
    containerReleaseSelector = binding.releaseSelector;


    Set<String> confKeys = ConfigHelper.sortedConfigKeys(publishedProviderConf);

    //  Add the -site configuration properties
    for (String key : confKeys) {
      String val = publishedProviderConf.get(key);
      clientProperties.put(key, val);
    }

    // set the cluster specification (once its dependency the client properties
    // is out the way
    setInitialInstanceDefinition(binding.instanceDefinition);

    //build the initial role list
    List<ProviderRole> roleList = new ArrayList<>(binding.roles);
    for (ProviderRole providerRole : roleList) {
      buildRole(providerRole);
    }

    ConfTreeOperations resources = instanceDefinition.getResourceOperations();

    Set<String> roleNames = resources.getComponentNames();
    for (String name : roleNames) {
      if (roles.containsKey(name)) {
        continue;
      }
      if (hasUniqueNames(resources, name)) {
        log.info("Skipping group {}", name);
        continue;
      }
      // this is a new value
      log.info("Adding role {}", name);
      MapOperations resComponent = resources.getComponent(name);
      ProviderRole dynamicRole = createDynamicProviderRole(name, resComponent);
      buildRole(dynamicRole);
      roleList.add(dynamicRole);
    }
    //then pick up the requirements
    buildRoleRequirementsFromResources();

    //set the livespan
    MapOperations globalResOpts = instanceDefinition.getResourceOperations().getGlobalOptions();

    startTimeThreshold = globalResOpts.getOptionInt(
        InternalKeys.INTERNAL_CONTAINER_FAILURE_SHORTLIFE,
        InternalKeys.DEFAULT_INTERNAL_CONTAINER_FAILURE_SHORTLIFE);

    failureThreshold = globalResOpts.getOptionInt(
        CONTAINER_FAILURE_THRESHOLD,
        DEFAULT_CONTAINER_FAILURE_THRESHOLD);
    nodeFailureThreshold = globalResOpts.getOptionInt(
        NODE_FAILURE_THRESHOLD,
        DEFAULT_NODE_FAILURE_THRESHOLD);
    initClusterStatus();


    // set up the role history
    roleHistory = new RoleHistory(roleStatusMap.values(), recordFactory);
    roleHistory.register(metricsAndMonitoring);
    roleHistory.onStart(binding.fs, binding.historyPath);
    // trigger first node update
    roleHistory.onNodesUpdated(binding.nodeReports);


    //rebuild any live containers
    rebuildModelFromRestart(binding.liveContainers);

    // any am config options to pick up
    logServerURL = binding.serviceConfig.get(YarnConfiguration.YARN_LOG_SERVER_URL, "");
    //mark as live
    applicationLive = true;
  }

  public void initClusterStatus() {
    //copy into cluster status. 
    ClusterDescription status = ClusterDescription.copy(clusterStatusTemplate);
    status.state = STATE_CREATED;
    MapOperations infoOps = new MapOperations("info", status.info);
    infoOps.mergeWithoutOverwrite(applicationInfo);
    SliderUtils.addBuildInfo(infoOps, "status");

    long now = now();
    status.setInfoTime(StatusKeys.INFO_LIVE_TIME_HUMAN,
                              StatusKeys.INFO_LIVE_TIME_MILLIS,
                              now);
    SliderUtils.setInfoTime(infoOps,
        StatusKeys.INFO_LIVE_TIME_HUMAN,
        StatusKeys.INFO_LIVE_TIME_MILLIS,
        now);
    if (0 == status.createTime) {
      status.createTime = now;
      SliderUtils.setInfoTime(infoOps,
          StatusKeys.INFO_CREATE_TIME_HUMAN,
          StatusKeys.INFO_CREATE_TIME_MILLIS,
          now);
    }
    status.state = STATE_LIVE;

      //set the app state to this status
    setClusterStatus(status);
  }

  /**
   * Build a dynamic provider role
   * @param name name of role
   * @return a new provider role
   * @throws BadConfigException bad configuration
   */
  public ProviderRole createDynamicProviderRole(String name, MapOperations component)
      throws BadConfigException {
    return createDynamicProviderRole(name, name, component);
  }

  /**
   * Build a dynamic provider role
   * @param name name of role
   * @param group group of role
   * @return a new provider role
   * @throws BadConfigException bad configuration
   */
  public ProviderRole createDynamicProviderRole(String name, String group, MapOperations component)
      throws BadConfigException {
    String priOpt = component.getMandatoryOption(COMPONENT_PRIORITY);
    int priority = SliderUtils.parseAndValidate(
        "value of " + name + " " + COMPONENT_PRIORITY, priOpt, 0, 1, -1);

    String placementOpt = component.getOption(COMPONENT_PLACEMENT_POLICY,
        Integer.toString(PlacementPolicy.DEFAULT));

    int placement = SliderUtils.parseAndValidate(
        "value of " + name + " " + COMPONENT_PLACEMENT_POLICY, placementOpt, 0, 0, -1);

    int placementTimeout = component.getOptionInt(PLACEMENT_ESCALATE_DELAY,
            DEFAULT_PLACEMENT_ESCALATE_DELAY_SECONDS);

    ProviderRole newRole = new ProviderRole(name,
        group,
        priority,
        placement,
        getNodeFailureThresholdForRole(group),
        placementTimeout,
        component.getOption(YARN_LABEL_EXPRESSION, DEF_YARN_LABEL_EXPRESSION));
    log.info("New {} ", newRole);
    return newRole;
  }

  /**
   * Actions to perform when an instance definition is updated
   * Currently: 
   * <ol>
   *   <li>
   *     resolve the configuration
   *   </li>
   *   <li>
   *     update the cluster spec derivative
   *   </li>
   * </ol>
   *  
   * @throws BadConfigException
   */
  private synchronized void onInstanceDefinitionUpdated()
      throws BadConfigException, IOException {

    log.debug("Instance definition updated");
    //note the time 
    snapshotTime = now();

    for (String component : instanceDefinition.getResourceOperations().getComponentNames()) {
      instanceDefinition.getAppConfOperations().getOrAddComponent(component);
    }

    // resolve references if not already done
    instanceDefinition.resolve();

    // force in the AM desired state values
    ConfTreeOperations resources = instanceDefinition.getResourceOperations();

    if (resources.getComponent(SliderKeys.COMPONENT_AM) != null) {
      resources.setComponentOpt(
          SliderKeys.COMPONENT_AM, COMPONENT_INSTANCES, "1");
    }


    //snapshot all three sectons
    resourcesSnapshot = ConfTreeOperations.fromInstance(instanceDefinition.getResources());
    appConfSnapshot = ConfTreeOperations.fromInstance(instanceDefinition.getAppConf());
    internalsSnapshot = ConfTreeOperations.fromInstance(instanceDefinition.getInternal());
    //build a new aggregate from the snapshots
    instanceDefinitionSnapshot = new AggregateConf(resourcesSnapshot.confTree,
                                                   appConfSnapshot.confTree,
                                                   internalsSnapshot.confTree);
    instanceDefinitionSnapshot.setName(instanceDefinition.getName());

    clusterStatusTemplate = ClusterDescriptionOperations.buildFromInstanceDefinition(
          instanceDefinition);

    // Add the -site configuration properties
    for (Map.Entry<String, String> prop : clientProperties.entrySet()) {
      clusterStatusTemplate.clientProperties.put(prop.getKey(), prop.getValue());
    }

  }

  /**
   * The resource configuration is updated -review and update state.
   * @param resources updated resources specification
   * @return a list of any dynamically added provider roles
   * (purely for testing purposes)
   */
  @VisibleForTesting
  public synchronized List<ProviderRole> updateResourceDefinitions(ConfTree resources)
      throws BadConfigException, IOException {
    log.debug("Updating resources to {}", resources);
    // snapshot the (possibly unresolved) values
    ConfTreeSerDeser serDeser = new ConfTreeSerDeser();
    unresolvedInstanceDefinition.setResources(
        serDeser.fromInstance(resources));
    // assign another copy under the instance definition for resolving
    // and then driving application size
    instanceDefinition.setResources(serDeser.fromInstance(resources));
    onInstanceDefinitionUpdated();

    // propagate the role table
    Map<String, Map<String, String>> updated = resources.components;
    getClusterStatus().roles = SliderUtils.deepClone(updated);
    getClusterStatus().updateTime = now();
    return buildRoleRequirementsFromResources();
  }

  /**
   * build the role requirements from the cluster specification
   * @return a list of any dynamically added provider roles
   */
  private List<ProviderRole> buildRoleRequirementsFromResources() throws BadConfigException {

    List<ProviderRole> newRoles = new ArrayList<>(0);

    // now update every role's desired count.
    // if there are no instance values, that role count goes to zero

    ConfTreeOperations resources =
        instanceDefinition.getResourceOperations();

    // Add all the existing roles
    Map<String, Integer> groupCounts = new HashMap<>();
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      if (roleStatus.isExcludeFromFlexing()) {
        // skip inflexible roles, e.g AM itself
        continue;
      }
      long currentDesired = roleStatus.getDesired();
      String role = roleStatus.getName();
      String roleGroup = roleStatus.getGroup();
      int desiredInstanceCount = getDesiredInstanceCount(resources, roleGroup);

      int newDesired = desiredInstanceCount;
      if (hasUniqueNames(resources, roleGroup)) {
        Integer groupCount = 0;
        if (groupCounts.containsKey(roleGroup)) {
          groupCount = groupCounts.get(roleGroup);
        }

        newDesired = desiredInstanceCount - groupCount;

        if (newDesired > 0) {
          newDesired = 1;
          groupCounts.put(roleGroup, groupCount + newDesired);
        } else {
          newDesired = 0;
        }
      }

      if (newDesired == 0) {
        log.info("Role {} has 0 instances specified", role);
      }
      if (currentDesired != newDesired) {
        log.info("Role {} flexed from {} to {}", role, currentDesired,
            newDesired);
        roleStatus.setDesired(newDesired);
      }
    }

    // now the dynamic ones. Iterate through the the cluster spec and
    // add any role status entries not in the role status
    Set<String> roleNames = resources.getComponentNames();
    for (String name : roleNames) {
      if (roles.containsKey(name)) {
        continue;
      }
      if (hasUniqueNames(resources, name)) {
        // THIS NAME IS A GROUP
        int desiredInstanceCount = getDesiredInstanceCount(resources, name);
        Integer groupCount = 0;
        if (groupCounts.containsKey(name)) {
          groupCount = groupCounts.get(name);
        }
        for (int i = groupCount + 1; i <= desiredInstanceCount; i++) {
          int priority = resources.getComponentOptInt(name, COMPONENT_PRIORITY, i);
          // this is a new instance of an existing group
          String newName = String.format("%s%d", name, i);
          int newPriority = getNewPriority(priority + i - 1);
          log.info("Adding new role {}", newName);
          MapOperations component = resources.getComponent(name,
              Collections.singletonMap(COMPONENT_PRIORITY,
                  Integer.toString(newPriority)));
          if (component == null) {
            throw new BadConfigException("Component is null for name = " + name
                + ", newPriority =" + newPriority);
          }
          ProviderRole dynamicRole = createDynamicProviderRole(newName, name, component);
          RoleStatus roleStatus = buildRole(dynamicRole);
          roleStatus.setDesired(1);
          log.info("New role {}", roleStatus);
          if (roleHistory != null) {
            roleHistory.addNewRole(roleStatus);
          }
          newRoles.add(dynamicRole);
        }
      } else {
        // this is a new value
        log.info("Adding new role {}", name);
        MapOperations component = resources.getComponent(name);
        ProviderRole dynamicRole = createDynamicProviderRole(name, component);
        RoleStatus roleStatus = buildRole(dynamicRole);
        roleStatus.setDesired(getDesiredInstanceCount(resources, name));
        log.info("New role {}", roleStatus);
        if (roleHistory != null) {
          roleHistory.addNewRole(roleStatus);
        }
        newRoles.add(dynamicRole);
      }
    }
    // and fill in all those roles with their requirements
    buildRoleResourceRequirements();

    return newRoles;
  }

  private int getNewPriority(int start) {
    if (!rolePriorityMap.containsKey(start)) {
      return start;
    }
    return rolePriorityMap.lastKey() + 1;
  }

  /**
   * Get the desired instance count of a role, rejecting negative values
   * @param resources resource map
   * @param roleGroup role group
   * @return the instance count
   * @throws BadConfigException if the count is negative
   */
  private int getDesiredInstanceCount(ConfTreeOperations resources,
      String roleGroup) throws BadConfigException {
    int desiredInstanceCount =
      resources.getComponentOptInt(roleGroup, COMPONENT_INSTANCES, 0);

    if (desiredInstanceCount < 0) {
      log.error("Role {} has negative desired instances : {}", roleGroup,
          desiredInstanceCount);
      throw new BadConfigException(
          "Negative instance count (%) requested for component %s",
          desiredInstanceCount, roleGroup);
    }
    return desiredInstanceCount;
  }

  private Boolean hasUniqueNames(ConfTreeOperations resources, String group) {
    MapOperations component = resources.getComponent(group);
    if (component == null) {
      log.info("Component was null for {} when checking unique names", group);
      return Boolean.FALSE;
    }
    return component.getOptionBool(UNIQUE_NAMES, Boolean.FALSE);
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
  public RoleStatus buildRole(ProviderRole providerRole) throws BadConfigException {
    // build role status map
    int priority = providerRole.id;
    if (roleStatusMap.containsKey(priority)) {
      throw new BadConfigException("Duplicate Provider Key: %s and %s",
                                   providerRole,
                                   roleStatusMap.get(priority));
    }
    RoleStatus roleStatus = new RoleStatus(providerRole);
    roleStatusMap.put(priority, roleStatus);
    String name = providerRole.name;
    roles.put(name, providerRole);
    rolePriorityMap.put(priority, providerRole);
    // register its entries
    metricsAndMonitoring.addMetricSet(MetricsConstants.PREFIX_SLIDER_ROLES + name, roleStatus);
    return roleStatus;
  }

  /**
   * Build up the requirements of every resource
   */
  private void buildRoleResourceRequirements() {
    for (RoleStatus role : roleStatusMap.values()) {
      role.setResourceRequirements(
          buildResourceRequirements(role, recordFactory.newResource()));
    }
  }

  /**
   * build up the special master node, which lives
   * in the live node set but has a lifecycle bonded to the AM
   * @param containerId the AM master
   * @param host hostname
   * @param amPort port
   * @param nodeHttpAddress http address: may be null
   */
  public void buildAppMasterNode(ContainerId containerId,
                                 String host,
                                 int amPort,
                                 String nodeHttpAddress) {
    Container container = new ContainerPBImpl();
    container.setId(containerId);
    NodeId nodeId = NodeId.newInstance(host, amPort);
    container.setNodeId(nodeId);
    container.setNodeHttpAddress(nodeHttpAddress);
    RoleInstance am = new RoleInstance(container);
    am.role = SliderKeys.COMPONENT_AM;
    am.group = SliderKeys.COMPONENT_AM;
    am.roleId = SliderKeys.ROLE_AM_PRIORITY_INDEX;
    am.createTime =now();
    am.startTime = am.createTime;
    appMasterNode = am;
    //it is also added to the set of live nodes
    getLiveContainers().put(containerId, am);
    putOwnedContainer(containerId, am);

    // patch up the role status
    RoleStatus roleStatus = roleStatusMap.get(SliderKeys.ROLE_AM_PRIORITY_INDEX);
    roleStatus.setDesired(1);
    roleStatus.incActual();
    roleStatus.incStarted();
  }

  /**
   * Note that the master node has been launched,
   * though it isn't considered live until any forked
   * processes are running. It is NOT registered with
   * the role history -the container is incomplete
   * and it will just cause confusion
   */
  public void noteAMLaunched() {
    getLiveContainers().put(appMasterNode.getContainerId(), appMasterNode);
  }

  /**
   * AM declares ourselves live in the cluster description.
   * This is meant to be triggered from the callback
   * indicating the spawned process is up and running.
   */
  public void noteAMLive() {
    appMasterNode.state = STATE_LIVE;
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
   * Get a deep clone of the role status list. Concurrent events may mean this
   * list (or indeed, some of the role status entries) may be inconsistent
   * @return a snapshot of the role status entries
   */
  public List<RoleStatus> cloneRoleStatusList() {
    Collection<RoleStatus> statuses = roleStatusMap.values();
    List<RoleStatus> statusList = new ArrayList<>(statuses.size());
    try {
      for (RoleStatus status : statuses) {
        statusList.add((RoleStatus)(status.clone()));
      }
    } catch (CloneNotSupportedException e) {
      log.warn("Unexpected cloning failure: {}", e, e);
    }
    return statusList;
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
   * Build a map of role->nodename->node-info
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
    RoleStatus role = lookupRoleStatus(instance.roleId);
    role.incReleasing();
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
      incrementRequestCount(role);
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
    incrementRequestCount(role);
    role.setOutstandingAArequest(request);
    return request.getIssuedRequest();
  }

  /**
   * Increment the request count of a role.
   * <p>
   *   Also updates application state counters
   * @param role role being requested.
   */
  protected void incrementRequestCount(RoleStatus role) {
    role.incRequested();
    incOutstandingContainerRequests();
  }

  /**
   * Inc #of outstanding requests.
   */
  private void incOutstandingContainerRequests() {
     outstandingContainerRequests.inc();
  }

  /**
   * Decrement the number of outstanding requests. This never goes below zero.
   */
  private void decOutstandingContainerRequests() {
    synchronized (outstandingContainerRequests) {
      if (outstandingContainerRequests.getCount() > 0) {
        // decrement but never go below zero
        outstandingContainerRequests.dec();
      }
    }
  }


  /**
   * Get the value of a YARN requirement (cores, RAM, etc).
   * These are returned as integers, but there is special handling of the 
   * string {@link ResourceKeys#YARN_RESOURCE_MAX}, which triggers
   * the return of the maximum value.
   * @param group component to get from
   * @param option option name
   * @param defVal default value
   * @param maxVal value to return if the max val is requested
   * @return parsed value
   * @throws NumberFormatException if the role could not be parsed.
   */
  private int getResourceRequirement(ConfTreeOperations resources,
                                     String group,
                                     String option,
                                     int defVal,
                                     int maxVal) {

    String val = resources.getComponentOpt(group, option,
        Integer.toString(defVal));
    Integer intVal;
    if (YARN_RESOURCE_MAX.equals(val)) {
      intVal = maxVal;
    } else {
      intVal = Integer.decode(val);
    }
    return intVal;
  }

  /**
   * Build up the resource requirements for this role from the
   * cluster specification, including substituing max allowed values
   * if the specification asked for it.
   * @param role role
   * @param capability capability to set up. A new one may be created
   * during normalization
   */
  public Resource buildResourceRequirements(RoleStatus role, Resource capability) {
    // Set up resource requirements from role values
    String name = role.getName();
    String group = role.getGroup();
    ConfTreeOperations resources = getResourcesSnapshot();
    int cores = getResourceRequirement(resources,
                                       group,
                                       YARN_CORES,
                                       DEF_YARN_CORES,
                                       containerMaxCores);
    capability.setVirtualCores(cores);
    int ram = getResourceRequirement(resources, group,
                                     YARN_MEMORY,
                                     DEF_YARN_MEMORY,
                                     containerMaxMemory);
    capability.setMemory(ram);
    log.debug("Component {} has RAM={}, vCores ={}", name, ram, cores);
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
    incStartedCountainerCount();
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
    RoleStatus roleStatus = lookupRoleStatus(instance.roleId);
    roleStatus.incStarted();
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
    incFailedCountainerCount();
    incStartFailedCountainerCount();
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
      roleStatus.noteFailed(true, text, ContainerOutcome.Failed);
      getFailedContainers().put(containerId, instance);
      roleHistory.onNodeManagerContainerStartFailed(instance.container);
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
    return new NodeUpdatedOutcome(false, new ArrayList<AbstractRMOperation>(0));
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
  public synchronized NodeCompletionResult onCompletedNode(ContainerStatus status) {
    ContainerId containerId = status.getContainerId();
    NodeCompletionResult result = new NodeCompletionResult();
    RoleInstance roleInstance;

    int exitStatus = status.getExitStatus();
    result.exitStatus = exitStatus;
    if (containersBeingReleased.containsKey(containerId)) {
      log.info("Container was queued for release : {}", containerId);
      Container container = containersBeingReleased.remove(containerId);
      RoleStatus roleStatus = lookupRoleStatus(container);
      long releasing = roleStatus.decReleasing();
      long actual = roleStatus.decActual();
      long completedCount = roleStatus.incCompleted();
      log.info("decrementing role count for role {} to {}; releasing={}, completed={}",
          roleStatus.getName(),
          actual,
          releasing,
          completedCount);
      result.outcome = ContainerOutcome.Completed;
      roleHistory.onReleaseCompleted(container);

    } else if (surplusNodes.remove(containerId)) {
      //its a surplus one being purged
      result.surplusNode = true;
    } else {
      // a container has failed or been killed
      // use the exit code to determine the outcome
      result.containerFailed = true;
      result.outcome = ContainerOutcome.fromExitStatus(exitStatus);

      roleInstance = removeOwnedContainer(containerId);
      if (roleInstance != null) {
        //it was active, move it to failed 
        incFailedCountainerCount();
        failedContainers.put(containerId, roleInstance);
      } else {
        // the container may have been noted as failed already, so look
        // it up
        roleInstance = failedContainers.get(containerId);
      }
      if (roleInstance != null) {
        int roleId = roleInstance.roleId;
        String rolename = roleInstance.role;
        log.info("Failed container in role[{}] : {}", roleId, rolename);
        try {
          RoleStatus roleStatus = lookupRoleStatus(roleId);
          roleStatus.decActual();
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
          roleStatus.noteFailed(shortLived, message, result.outcome);
          long failed = roleStatus.getFailed();
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
      actual += role.getActual();
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

  public ClusterDescription refreshClusterStatus() {
    return refreshClusterStatus(null);
  }

  /**
   * Update the cluster description with the current application state
   * @param providerStatus status from the provider for the cluster info section
   */
  public synchronized ClusterDescription refreshClusterStatus(Map<String, String> providerStatus) {
    ClusterDescription cd = getClusterStatus();
    long now = now();
    cd.setInfoTime(StatusKeys.INFO_STATUS_TIME_HUMAN,
                   StatusKeys.INFO_STATUS_TIME_MILLIS,
                   now);
    if (providerStatus != null) {
      for (Map.Entry<String, String> entry : providerStatus.entrySet()) {
        cd.setInfo(entry.getKey(), entry.getValue());
      }
    }
    MapOperations infoOps = new MapOperations("info", cd.info);
    infoOps.mergeWithoutOverwrite(applicationInfo);
    SliderUtils.addBuildInfo(infoOps, "status");
    cd.statistics = new HashMap<>();

    // build the map of node -> container IDs
    Map<String, List<String>> instanceMap = createRoleToInstanceMap();
    cd.instances = instanceMap;
    
    //build the map of node -> containers
    Map<String, Map<String, ClusterNode>> clusterNodes =
      createRoleToClusterNodeMap();
    log.info("app state clusterNodes {} ", clusterNodes.toString());
    cd.status = new HashMap<>();
    cd.status.put(ClusterDescriptionKeys.KEY_CLUSTER_LIVE, clusterNodes);


    for (RoleStatus role : getRoleStatusMap().values()) {
      String rolename = role.getName();
      if (hasUniqueNames(instanceDefinition.getResourceOperations(),
          role.getGroup())) {
        cd.setRoleOpt(rolename, COMPONENT_PRIORITY, role.getPriority());
        cd.setRoleOpt(rolename, ROLE_GROUP, role.getGroup());
        MapOperations groupOptions = instanceDefinition.getResourceOperations()
            .getComponent(role.getGroup());
        SliderUtils.mergeMapsIgnoreDuplicateKeys(cd.getRole(rolename),
            groupOptions.options);
      }
      String prefix = instanceDefinition.getAppConfOperations()
          .getComponentOpt(role.getGroup(), ROLE_PREFIX, null);
      if (SliderUtils.isSet(prefix)) {
        cd.setRoleOpt(rolename, ROLE_PREFIX, SliderUtils.trimPrefix(prefix));
      }
      List<String> instances = instanceMap.get(rolename);
      int nodeCount = instances != null ? instances.size(): 0;
      cd.setRoleOpt(rolename, COMPONENT_INSTANCES,
                    role.getDesired());
      cd.setRoleOpt(rolename, ROLE_ACTUAL_INSTANCES, nodeCount);
      cd.setRoleOpt(rolename, ROLE_REQUESTED_INSTANCES, role.getRequested());
      cd.setRoleOpt(rolename, ROLE_RELEASING_INSTANCES, role.getReleasing());
      cd.setRoleOpt(rolename, ROLE_FAILED_INSTANCES, role.getFailed());
      cd.setRoleOpt(rolename, ROLE_FAILED_STARTING_INSTANCES, role.getStartFailed());
      cd.setRoleOpt(rolename, ROLE_FAILED_RECENTLY_INSTANCES, role.getFailedRecently());
      cd.setRoleOpt(rolename, ROLE_NODE_FAILED_INSTANCES, role.getNodeFailed());
      cd.setRoleOpt(rolename, ROLE_PREEMPTED_INSTANCES, role.getPreempted());
      if (role.isAntiAffinePlacement()) {
        cd.setRoleOpt(rolename, ROLE_PENDING_AA_INSTANCES, role.getPendingAntiAffineRequests());
      }
      Map<String, Integer> stats = role.buildStatistics();
      cd.statistics.put(rolename, stats);
    }

    Map<String, Integer> sliderstats = getLiveStatistics();
    cd.statistics.put(SliderKeys.COMPONENT_AM, sliderstats);

    // liveness
    cd.liveness = getApplicationLivenessInformation();

    return cd;
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
    li.activeRequests = (int)stats.requested;
    return li;
  }

  /**
   * Get the live statistics map
   * @return a map of statistics values, defined in the {@link StatusKeys}
   * keylist.
   */
  protected Map<String, Integer> getLiveStatistics() {
    Map<String, Integer> sliderstats = new HashMap<>();
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_LIVE,
        liveNodes.size());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_COMPLETED,
        completedContainerCount.intValue());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_FAILED,
        failedContainerCount.intValue());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_STARTED,
        startedContainers.intValue());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_START_FAILED,
         startFailedContainerCount.intValue());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_SURPLUS,
        surplusContainers.intValue());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_UNKNOWN_COMPLETED,
        completionOfUnknownContainerEvent.get());
    return sliderstats;
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

  /**
   * Look at where the current node state is -and whether it should be changed
   */
  public synchronized List<AbstractRMOperation> reviewRequestAndReleaseNodes()
      throws SliderInternalStateException, TriggerClusterTeardownException {
    log.debug("in reviewRequestAndReleaseNodes()");
    List<AbstractRMOperation> allOperations = new ArrayList<>();
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      if (!roleStatus.isExcludeFromFlexing()) {
        List<AbstractRMOperation> operations = reviewOneRole(roleStatus);
        allOperations.addAll(operations);
      }
    }
    return allOperations;
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

    if (failures > threshold) {
      throw new TriggerClusterTeardownException(
        SliderExitCodes.EXIT_DEPLOYMENT_FAILED,
          FinalApplicationStatus.FAILED, ErrorStrings.E_UNSTABLE_CLUSTER +
        " - failed with component %s failed 'recently' %d times (%d in startup);" +
        " threshold is %d - last failure: %s",
          role.getName(),
        role.getFailed(),
        role.getStartFailed(),
          threshold,
        role.getFailureMessage());
    }
  }

  /**
   * Get the failure threshold for a specific role, falling back to
   * the global one if not
   * @param roleStatus role
   * @return the threshold for failures
   */
  private int getFailureThresholdForRole(RoleStatus roleStatus) {
    ConfTreeOperations resources =
        instanceDefinition.getResourceOperations();
    return resources.getComponentOptInt(roleStatus.getGroup(),
        CONTAINER_FAILURE_THRESHOLD,
        failureThreshold);
  }

  /**
   * Get the node failure threshold for a specific role, falling back to
   * the global one if not
   * @param roleGroup role group
   * @return the threshold for failures
   */
  private int getNodeFailureThresholdForRole(String roleGroup) {
    ConfTreeOperations resources =
        instanceDefinition.getResourceOperations();
    return resources.getComponentOptInt(roleGroup,
                                        NODE_FAILURE_THRESHOLD,
                                        nodeFailureThreshold);
  }

  /**
   * Reset the "recent" failure counts of all roles
   */
  public void resetFailureCounts() {
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      long failed = roleStatus.resetFailedRecently();
      log.info("Resetting failure count of {}; was {}",
               roleStatus.getName(),
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

    log.info("Reviewing {} : ", role);
    log.debug("Expected {}, Delta: {}", expected, delta);
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
              addContainerRequest(operations, request);
            } else {
              log.info("No location for anti-affine request");
            }
          }
        } else {
          log.warn("Awaiting node map before generating anti-affinity requests");
        }
        log.info("Setting pending to {}", pending);
        role.setPendingAntiAffineRequests(pending);
      } else {

        for (int i = 0; i < delta; i++) {
          //get the role history to select a suitable node, if available
          addContainerRequest(operations, createContainerRequest(role));
        }
      }
    } else if (delta < 0) {
      log.info("{}: Asking for {} fewer node(s) for a total of {}", name,
               -delta,
               expected);
      // reduce the number expected (i.e. subtract the delta)
      long excess = -delta;

      // how many requests are outstanding? for AA roles, this includes pending
      long outstandingRequests = role.getRequested() + role.getPendingAntiAffineRequests();
      if (outstandingRequests > 0) {
        // outstanding requests.
        int toCancel = (int)Math.min(outstandingRequests, excess);

        // Delegate to Role History
        List<AbstractRMOperation> cancellations = roleHistory.cancelRequestsForRole(role, toCancel);
        log.info("Found {} outstanding requests to cancel", cancellations.size());
        operations.addAll(cancellations);
        if (toCancel != cancellations.size()) {
          log.error("Tracking of outstanding requests is not in sync with the summary statistics:" +
              " expected to be able to cancel {} requests, but got {}",
              toCancel, cancellations.size());
        }

        role.cancel(toCancel);
        excess -= toCancel;
        assert excess >= 0 : "Attempted to cancel too many requests";
        log.info("Submitted {} cancellations, leaving {} to release",
            toCancel, excess);
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
          operations.add(new ContainerReleaseOperation(possible.getId()));
        }
      }

    } else {
      // actual + requested == desired
      // there's a special case here: clear all pending AA requests
      if (role.getPendingAntiAffineRequests() > 0) {
        log.debug("Clearing outstanding pending AA requests");
        role.setPendingAntiAffineRequests(0);
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
      AMRMClient.ContainerRequest containerAsk) {
    if (containerAsk != null) {
      log.info("Container ask is {} and label = {}", containerAsk,
          containerAsk.getNodeLabelExpression());
      int askMemory = containerAsk.getCapability().getMemory();
      if (askMemory > this.containerMaxMemory) {
        log.warn("Memory requested: {} > max of {}", askMemory, containerMaxMemory);
      }
      operations.add(new ContainerRequestOperation(containerAsk));
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
        operations.add(new ContainerReleaseOperation(role.getId()));
      }
    }

    return operations;
  }

  /**
   * Find a container running on a specific host -looking
   * into the node ID to determine this.
   *
   * @param node node
   * @param roleId role the container must be in
   * @return a container or null if there are no containers on this host
   * that can be released.
   */
  private RoleInstance findRoleInstanceOnHost(NodeInstance node, int roleId) {
    Collection<RoleInstance> targets = cloneOwnedContainerList();
    String hostname = node.hostname;
    for (RoleInstance ri : targets) {
      if (hostname.equals(RoleHistoryUtils.hostnameOf(ri.container))
                         && ri.roleId == roleId
        && containersBeingReleased.get(ri.getContainerId()) == null) {
        return ri;
      }
    }
    return null;
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
  public synchronized void onContainersAllocated(List<Container> allocatedContainers,
                                    List<ContainerAssignment> assignments,
                                    List<AbstractRMOperation> operations) {
    assignments.clear();
    operations.clear();
    List<Container> ordered = roleHistory.prepareAllocationList(allocatedContainers);
    log.debug("onContainersAllocated(): Total containers allocated = {}", ordered.size());
    for (Container container : ordered) {
      final NodeId nodeId = container.getNodeId();
      String containerHostInfo = nodeId.getHost() + ":" + nodeId.getPort();
      //get the role
      final ContainerId cid = container.getId();
      final RoleStatus role = lookupRoleStatus(container);

      //dec requested count
      role.decRequested();

      //inc allocated count -this may need to be dropped in a moment,
      // but us needed to update the logic below
      final long allocated = role.incActual();
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
        surplusNodes.add(cid);
        surplusContainers.inc();
        //and, as we aren't binding it to role, dec that role's actual count
        role.decActual();
      } else {

        // Allocation being accepted -so decrement the number of outstanding requests
        decOutstandingContainerRequests();

        log.info("Assigning role {} to container" +
                 " {}," +
                 " on {}:{},",
                 roleName,
                 cid,
                 nodeId.getHost(),
                 nodeId.getPort());

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
          if (role.getPendingAntiAffineRequests() > 0) {
            // still an outstanding AA request: need to issue a new one.
            log.info("Asking for next container for AA role {}", roleName);
            if (!addContainerRequest(operations, createAAContainerRequest(role))) {
              log.info("No capacity in cluster for new requests");
            } else {
              role.decPendingAntiAffineRequests();
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
   * Get diagnostics info about containers
   */
  public String getContainerDiagnosticInfo() {
    StringBuilder builder = new StringBuilder();
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      builder.append(roleStatus).append('\n');
    }
    return builder.toString();
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
    clusterStatus.setInfo(StatusKeys.INFO_CONTAINERS_AM_RESTART,
                               Integer.toString(liveContainers.size()));
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
    RoleStatus role =
      lookupRoleStatus(roleId);
    // increment its count
    role.incActual();
    String roleName = role.getName();
    
    log.info("Rebuilding container {} in role {} on {},",
             cid,
             roleName,
             containerHostInfo);

    //update app state internal structures and maps

    RoleInstance instance = new RoleInstance(container);
    instance.command = roleName;
    instance.role = roleName;
    instance.group = role.getGroup();
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
    sb.append(", startedContainers=").append(startedContainers);
    sb.append(", startFailedContainerCount=").append(startFailedContainerCount);
    sb.append(", surplusContainers=").append(surplusContainers);
    sb.append(", failedContainerCount=").append(failedContainerCount);
    sb.append(", outstanding non-AA Container Requests=")
        .append(outstandingContainerRequests);
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
}
