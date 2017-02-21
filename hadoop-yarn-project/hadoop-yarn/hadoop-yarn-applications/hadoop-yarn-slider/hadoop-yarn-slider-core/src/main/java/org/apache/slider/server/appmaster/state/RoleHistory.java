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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.appmaster.management.BoolMetric;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.management.Timestamp;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.UpdateBlacklistOperation;
import org.apache.slider.server.avro.LoadedRoleHistory;
import org.apache.slider.server.avro.NodeEntryRecord;
import org.apache.slider.server.avro.RoleHistoryHeader;
import org.apache.slider.server.avro.RoleHistoryWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Role History.
 * <p>
 * Synchronization policy: all public operations are synchronized.
 * Protected methods are in place for testing -no guarantees are made.
 * <p>
 * Inner classes have no synchronization guarantees; they should be manipulated 
 * in these classes and not externally.
 * <p>
 * Note that as well as some methods marked visible for testing, there
 * is the option for the time generator method, {@link #now()} to
 * be overridden so that a repeatable time series can be used.
 * 
 */
public class RoleHistory {
  protected static final Logger log =
    LoggerFactory.getLogger(RoleHistory.class);
  private final List<ProviderRole> providerRoles;
  /** the roles in here are shared with App State */
  private final Map<Integer, RoleStatus> roleStatusMap = new HashMap<>();
  private final AbstractClusterServices recordFactory;

  private long startTime;

  /** Time when saved */
  private final Timestamp saveTime = new Timestamp(0);

  /** If the history was loaded, the time at which the history was saved.
   * That is: the time the data was valid */
  private final Timestamp thawedDataTime = new Timestamp(0);
  
  private NodeMap nodemap;
  private int roleSize;
  private final BoolMetric dirty = new BoolMetric(false);
  private FileSystem filesystem;
  private Path historyPath;
  private RoleHistoryWriter historyWriter = new RoleHistoryWriter();

  /**
   * When were the nodes updated in a {@link #onNodesUpdated(List)} call?
   * If zero: never.
   */
  private final Timestamp nodesUpdatedTime = new Timestamp(0);
  private final BoolMetric nodeUpdateReceived = new BoolMetric(false);

  private OutstandingRequestTracker outstandingRequests =
    new OutstandingRequestTracker();

  /**
   * For each role, lists nodes that are available for data-local allocation,
   * ordered by more recently released - to accelerate node selection.
   * That is, they are "recently used nodes"
   */
  private Map<Integer, LinkedList<NodeInstance>> recentNodes;

  /**
   * Instantiate
   * @param roles initial role list
   * @param recordFactory yarn record factory
   * @throws BadConfigException
   */
  public RoleHistory(Collection<RoleStatus> roles, AbstractClusterServices recordFactory) throws BadConfigException {
    this.recordFactory = recordFactory;
    roleSize = roles.size();
    providerRoles = new ArrayList<>(roleSize);
    for (RoleStatus role : roles) {
      addNewRole(role);
    }
    reset();
  }

  /**
   * Reset the variables -this does not adjust the fixed attributes
   * of the history, but the nodemap and failed node map are cleared.
   */
  protected synchronized void reset() throws BadConfigException {

    nodemap = new NodeMap(roleSize);
    resetAvailableNodeLists();
    outstandingRequests = new OutstandingRequestTracker();
  }

  /**
   * Register all metrics with the metrics infra
   * @param metrics metrics
   */
  public void register(MetricsAndMonitoring metrics) {
    metrics.register(RoleHistory.class, dirty, "dirty");
    metrics.register(RoleHistory.class, nodesUpdatedTime, "nodes-updated.time");
    metrics.register(RoleHistory.class, nodeUpdateReceived, "nodes-updated.flag");
    metrics.register(RoleHistory.class, thawedDataTime, "thawed.time");
    metrics.register(RoleHistory.class, saveTime, "saved.time");
  }

  /**
   * safety check: make sure the role is unique amongst
   * the role stats...which is extended with the new role
   * @param roleStatus role
   * @throws ArrayIndexOutOfBoundsException
   * @throws BadConfigException
   */
  protected void putRole(RoleStatus roleStatus) throws BadConfigException {
    int index = roleStatus.getKey();
    if (index < 0) {
      throw new BadConfigException("Provider " + roleStatus + " id is out of range");
    }
    if (roleStatusMap.get(index) != null) {
      throw new BadConfigException(
        roleStatus.toString() + " id duplicates that of " +
            roleStatusMap.get(index));
    }
    roleStatusMap.put(index, roleStatus);
  }

  /**
   * Add a new role
   * @param roleStatus new role
   */
  public void addNewRole(RoleStatus roleStatus) throws BadConfigException {
    log.debug("Validating/adding new role to role history: {} ", roleStatus);
    putRole(roleStatus);
    this.providerRoles.add(roleStatus.getProviderRole());
  }

  /**
   * Lookup a role by ID
   * @param roleId role Id
   * @return role or null if not found
   */
  public ProviderRole lookupRole(int roleId) {
    for (ProviderRole role : providerRoles) {
      if (role.id == roleId) {
        return role;
      }
    }
    return null;
  }

  /**
   * Clear the lists of available nodes
   */
  private synchronized void resetAvailableNodeLists() {
    recentNodes = new ConcurrentHashMap<>(roleSize);
  }

  /**
   * Prepare the history for re-reading its state.
   * <p>
   * This intended for use by the RoleWriter logic.
   * @throws BadConfigException if there is a problem rebuilding the state
   */
  private void prepareForReading(RoleHistoryHeader header)
      throws BadConfigException {
    reset();

    int roleCountInSource = header.getRoles();
    if (roleCountInSource != roleSize) {
      log.warn("Number of roles in source {}"
              +" does not match the expected number of {}",
          roleCountInSource,
          roleSize);
    }
    //record when the data was loaded
    setThawedDataTime(header.getSaved());
  }

  /**
   * rebuild the placement history from the loaded role history
   * @param loadedRoleHistory loaded history
   * @return the number of entries discarded
   * @throws BadConfigException if there is a problem rebuilding the state
   */
  @VisibleForTesting
  public synchronized int rebuild(LoadedRoleHistory loadedRoleHistory) throws BadConfigException {
    RoleHistoryHeader header = loadedRoleHistory.getHeader();
    prepareForReading(header);
    int discarded = 0;
    Long saved = header.getSaved();
    for (NodeEntryRecord nodeEntryRecord : loadedRoleHistory.records) {
      Integer roleId = nodeEntryRecord.getRole();
      NodeEntry nodeEntry = new NodeEntry(roleId);
      nodeEntry.setLastUsed(nodeEntryRecord.getLastUsed());
      if (nodeEntryRecord.getActive()) {
        //if active at the time of save, make the last used time the save time
        nodeEntry.setLastUsed(saved);
      }
      String hostname = SliderUtils.sequenceToString(nodeEntryRecord.getHost());
      ProviderRole providerRole = lookupRole(roleId);
      if (providerRole == null) {
        // discarding entry
        log.info("Discarding history entry with unknown role: {} on host {}",
            roleId, hostname);
        discarded ++;
      } else {
        NodeInstance instance = getOrCreateNodeInstance(hostname);
        instance.set(roleId, nodeEntry);
      }
    }
    return discarded;
  }

  public synchronized long getStartTime() {
    return startTime;
  }

  public synchronized long getSaveTime() {
    return saveTime.get();
  }

  public long getThawedDataTime() {
    return thawedDataTime.get();
  }

  public void setThawedDataTime(long thawedDataTime) {
    this.thawedDataTime.set(thawedDataTime);
  }

  public synchronized int getRoleSize() {
    return roleSize;
  }

  /**
   * Get the total size of the cluster -the number of NodeInstances
   * @return a count
   */
  public synchronized int getClusterSize() {
    return nodemap.size();
  }

  public synchronized boolean isDirty() {
    return dirty.get();
  }

  public synchronized void setDirty(boolean dirty) {
    this.dirty.set(dirty);
  }

  /**
   * Tell the history that it has been saved; marks itself as clean
   * @param timestamp timestamp -updates the savetime field
   */
  public synchronized void saved(long timestamp) {
    setDirty(false);
    saveTime.set(timestamp);
  }

  /**
   * Get a clone of the nodemap.
   * The instances inside are not cloned
   * @return the map
   */
  public synchronized NodeMap cloneNodemap() {
    return (NodeMap) nodemap.clone();
  }

  /**
   * Get snapshot of the node map
   * @return a snapshot of the current node state
   * @param naming naming map of priority to enty name; entries must be unique.
   * It's OK to be incomplete, for those the list falls back to numbers.
   */
  public synchronized Map<String, NodeInformation> getNodeInformationSnapshot(
    Map<Integer, String> naming) {
    Map<String, NodeInformation> result = new HashMap<>(nodemap.size());
    for (Map.Entry<String, NodeInstance> entry : nodemap.entrySet()) {
      result.put(entry.getKey(), entry.getValue().serialize(naming));
    }
    return result;
  }

  /**
   * Get the information on a node
   * @param hostname hostname
   * @param naming naming map of priority to enty name; entries must be unique.
   * It's OK to be incomplete, for those the list falls back to numbers.
   * @return the information about that host, or null if there is none
   */
  public synchronized NodeInformation getNodeInformation(String hostname,
    Map<Integer, String> naming) {
    NodeInstance nodeInstance = nodemap.get(hostname);
    return nodeInstance != null ? nodeInstance.serialize(naming) : null;
  }

  /**
   * Get the node instance for the specific node -creating it if needed
   * @param hostname node address
   * @return the instance
   */
  public synchronized NodeInstance getOrCreateNodeInstance(String hostname) {
    //convert to a string
    return nodemap.getOrCreate(hostname);
  }

  /**
   * Insert a list of nodes into the map; overwrite any with that name.
   * This is a bulk operation for testing.
   * Important: this does not update the available node lists, these
   * must be rebuilt afterwards.
   * @param nodes collection of nodes.
   */
  @VisibleForTesting
  public synchronized void insert(Collection<NodeInstance> nodes) {
    nodemap.insert(nodes);
  }

  /**
   * Get current time. overrideable for test subclasses
   * @return current time in millis
   */
  protected long now() {
    return System.currentTimeMillis();
  }

  /**
   * Mark ourselves as dirty
   */
  public void touch() {
    setDirty(true);
    try {
      saveHistoryIfDirty();
    } catch (IOException e) {
      log.warn("Failed to save history file ", e);
    }
  }

  /**
   * reset the failed recently counters
   */
  public synchronized void resetFailedRecently() {
    log.info("Resetting failure history");
    nodemap.resetFailedRecently();
  }

  /**
   * Get the path used for history files
   * @return the directory used for history files
   */
  public Path getHistoryPath() {
    return historyPath;
  }

  /**
   * Save the history to its location using the timestamp as part of
   * the filename. The saveTime and dirty fields are updated
   * @param time timestamp timestamp to use as the save time
   * @return the path saved to
   * @throws IOException IO problems
   */
  @VisibleForTesting
  public synchronized Path saveHistory(long time) throws IOException {
    Path filename = historyWriter.createHistoryFilename(historyPath, time);
    historyWriter.write(filesystem, filename, true, this, time);
    saved(time);
    return filename;
  }

  /**
   * Save the history with the current timestamp if it is dirty;
   * return the path saved to if this is the case
   * @return the path or null if the history was not saved
   * @throws IOException failed to save for some reason
   */
  public synchronized Path saveHistoryIfDirty() throws IOException {
    if (isDirty()) {
      return saveHistory(now());
    } else {
      return null;
    }
  } 

  /**
   * Start up
   * @param fs filesystem 
   * @param historyDir path in FS for history
   * @return true if the history was thawed
   */
  public boolean onStart(FileSystem fs, Path historyDir) throws BadConfigException {
    assert filesystem == null;
    filesystem = fs;
    historyPath = historyDir;
    startTime = now();
    //assume the history is being thawed; this will downgrade as appropriate
    return onThaw();
    }
  
  /**
   * Handler for bootstrap event: there was no history to thaw
   */
  public void onBootstrap() {
    log.debug("Role history bootstrapped");
  }

  /**
   * Handle the start process <i>after the history has been rebuilt</i>,
   * and after any gc/purge
   */
  public synchronized boolean onThaw() throws BadConfigException {
    assert filesystem != null;
    assert historyPath != null;
    boolean thawSuccessful = false;
    //load in files from data dir

    LoadedRoleHistory loadedRoleHistory = null;
    try {
      loadedRoleHistory = historyWriter.loadFromHistoryDir(filesystem, historyPath);
    } catch (IOException e) {
      log.warn("Exception trying to load history from {}", historyPath, e);
    }
    if (loadedRoleHistory != null) {
      rebuild(loadedRoleHistory);
      thawSuccessful = true;
      Path loadPath = loadedRoleHistory.getPath();
      log.debug("loaded history from {}", loadPath);
      // delete any old entries
      try {
        int count = historyWriter.purgeOlderHistoryEntries(filesystem, loadPath);
        log.debug("Deleted {} old history entries", count);
      } catch (IOException e) {
        log.info("Ignoring exception raised while trying to delete old entries",
                 e);
      }

      //start is then completed
      buildRecentNodeLists();
    } else {
      //fallback to bootstrap procedure
      onBootstrap();
    }
    return thawSuccessful;
  }


  /**
   * (After the start), rebuild the availability data structures
   */
  @VisibleForTesting
  public synchronized void buildRecentNodeLists() {
    resetAvailableNodeLists();
    // build the list of available nodes
    for (Map.Entry<String, NodeInstance> entry : nodemap.entrySet()) {
      NodeInstance ni = entry.getValue();
      for (int i = 0; i < roleSize; i++) {
        NodeEntry nodeEntry = ni.get(i);
        if (nodeEntry != null && nodeEntry.isAvailable()) {
          log.debug("Adding {} for role {}", ni, i);
          listRecentNodesForRoleId(i).add(ni);
        }
      }
    }
    // sort the resulting arrays
    for (int i = 0; i < roleSize; i++) {
      sortRecentNodeList(i);
    }
  }

  /**
   * Get the nodes for an ID -may be null
   * @param id role ID
   * @return potentially null list
   */
  @VisibleForTesting
  public List<NodeInstance> getRecentNodesForRoleId(int id) {
    return recentNodes.get(id);
  }

  /**
   * Get a possibly empty list of suggested nodes for a role.
   * @param id role ID
   * @return list
   */
  private LinkedList<NodeInstance> listRecentNodesForRoleId(int id) {
    LinkedList<NodeInstance> instances = recentNodes.get(id);
    if (instances == null) {
      synchronized (this) {
        // recheck in the synchronized block and recreate
        if (recentNodes.get(id) == null) {
          recentNodes.put(id, new LinkedList<NodeInstance>());
        }
        instances = recentNodes.get(id);
      }
    }
    return instances;
  }

  /**
   * Sort a the recent node list for a single role
   * @param role role to sort
   */
  private void sortRecentNodeList(int role) {
    List<NodeInstance> nodesForRoleId = getRecentNodesForRoleId(role);
    if (nodesForRoleId != null) {
      Collections.sort(nodesForRoleId, new NodeInstance.Preferred(role));
    }
  }

  public synchronized UpdateBlacklistOperation updateBlacklist(
      Collection<RoleStatus> roleStatuses) {
    List<String> blacklistAdditions = new ArrayList<>();
    List<String> blacklistRemovals = new ArrayList<>();
    for (Entry<String, NodeInstance> nodeInstanceEntry : nodemap.entrySet()) {
      boolean shouldBeBlacklisted = false;
      String nodeHost = nodeInstanceEntry.getKey();
      NodeInstance nodeInstance = nodeInstanceEntry.getValue();
      for (RoleStatus roleStatus : roleStatuses) {
        if (nodeInstance.exceedsFailureThreshold(roleStatus)) {
          shouldBeBlacklisted = true;
          break;
        }
      }
      if (shouldBeBlacklisted) {
        if (!nodeInstance.isBlacklisted()) {
          blacklistAdditions.add(nodeHost);
          nodeInstance.setBlacklisted(true);
        }
      } else {
        if (nodeInstance.isBlacklisted()) {
          blacklistRemovals.add(nodeHost);
          nodeInstance.setBlacklisted(false);
        }
      }
    }
    if (blacklistAdditions.isEmpty() && blacklistRemovals.isEmpty()) {
      return null;
    }
    return new UpdateBlacklistOperation(blacklistAdditions, blacklistRemovals);
  }

  /**
   * Find a node for use
   * @param role role
   * @return the instance, or null for none
   */
  @VisibleForTesting
  public synchronized NodeInstance findRecentNodeForNewInstance(RoleStatus role) {
    if (!role.isPlacementDesired()) {
      // no data locality policy
      return null;
    }
    int roleId = role.getKey();
    boolean strictPlacement = role.isStrictPlacement();
    NodeInstance nodeInstance = null;
    // Get the list of possible targets.
    // This is a live list: changes here are preserved
    List<NodeInstance> targets = getRecentNodesForRoleId(roleId);
    if (targets == null) {
      // nothing to allocate on
      return null;
    }

    int cnt = targets.size();
    log.debug("There are {} node(s) to consider for {}", cnt, role.getName());
    for (int i = 0; i < cnt  && nodeInstance == null; i++) {
      NodeInstance candidate = targets.get(i);
      if (candidate.getActiveRoleInstances(roleId) == 0) {
        // no active instances: check failure statistics
        if (strictPlacement
            || (candidate.isOnline() && !candidate.exceedsFailureThreshold(role))) {
          targets.remove(i);
          // exit criteria for loop is now met
          nodeInstance = candidate;
        } else {
          // too many failures for this node
          log.info("Recent node failures is higher than threshold {}. Not requesting host {}",
              role.getNodeFailureThreshold(), candidate.hostname);
        }
      }
    }

    if (nodeInstance == null) {
      log.info("No node found for {}", role.getName());
    }
    return nodeInstance;
  }

  /**
   * Find a node for use
   * @param role role
   * @return the instance, or null for none
   */
  @VisibleForTesting
  public synchronized List<NodeInstance> findNodeForNewAAInstance(RoleStatus role) {
    // all nodes that are live and can host the role; no attempt to exclude ones
    // considered failing
    return nodemap.findAllNodesForRole(role.getKey(), role.getLabelExpression());
  }

  /**
   * Request an instance on a given node.
   * An outstanding request is created & tracked, with the 
   * relevant node entry for that role updated.
   *<p>
   * The role status entries will also be tracked
   * <p>
   * Returns the request that is now being tracked.
   * If the node instance is not null, it's details about the role is incremented
   *
   * @param node node to target or null for "any"
   * @param role role to request
   * @return the request
   */
  public synchronized OutstandingRequest requestInstanceOnNode(
      NodeInstance node, RoleStatus role, Resource resource) {
    OutstandingRequest outstanding = outstandingRequests.newRequest(node, role.getKey());
    outstanding.buildContainerRequest(resource, role, now());
    return outstanding;
  }

  /**
   * Find a node for a role and request an instance on that (or a location-less
   * instance)
   * @param role role status
   * @return a request ready to go, or null if this is an AA request and no
   * location can be found.
   */
  public synchronized OutstandingRequest requestContainerForRole(RoleStatus role) {

    if (role.isAntiAffinePlacement()) {
      return requestContainerForAARole(role);
    } else {
      Resource resource = recordFactory.newResource();
      role.copyResourceRequirements(resource);
      NodeInstance node = findRecentNodeForNewInstance(role);
      return requestInstanceOnNode(node, role, resource);
    }
  }

  /**
   * Find a node for an AA role and request an instance on that (or a location-less
   * instance)
   * @param role role status
   * @return a request ready to go, or null if no location can be found.
   */
  public synchronized OutstandingRequest requestContainerForAARole(RoleStatus role) {
    List<NodeInstance> nodes = findNodeForNewAAInstance(role);
    if (!nodes.isEmpty()) {
      OutstandingRequest outstanding = outstandingRequests.newAARequest(
          role.getKey(), nodes, role.getLabelExpression());
      Resource resource = recordFactory.newResource();
      role.copyResourceRequirements(resource);
      outstanding.buildContainerRequest(resource, role, now());
      return outstanding;
    } else {
      log.warn("No suitable location for {}", role.getName());
      return null;
    }
  }
  /**
   * Get the list of active nodes ... walks the node map so
   * is {@code O(nodes)}
   * @param role role index
   * @return a possibly empty list of nodes with an instance of that node
   */
  public synchronized List<NodeInstance> listActiveNodes(int role) {
    return nodemap.listActiveNodes(role);
  }

  /**
   * Get the node entry of a container
   * @param container container to look up
   * @return the entry
   * @throws RuntimeException if the container has no hostname
   */
  public NodeEntry getOrCreateNodeEntry(Container container) {
    return getOrCreateNodeInstance(container).getOrCreate(container);
  }

  /**
   * Get the node instance of a container -always returns something
   * @param container container to look up
   * @return a (possibly new) node instance
   * @throws RuntimeException if the container has no hostname
   */
  public synchronized NodeInstance getOrCreateNodeInstance(Container container) {
    return nodemap.getOrCreate(RoleHistoryUtils.hostnameOf(container));
  }

  /**
   * Get the node instance of a host if defined
   * @param hostname hostname to look up
   * @return a node instance or null
   * @throws RuntimeException if the container has no hostname
   */
  public synchronized NodeInstance getExistingNodeInstance(String hostname) {
    return nodemap.get(hostname);
  }

  /**
   * Get the node instance of a container <i>if there's an entry in the history</i>
   * @param container container to look up
   * @return a node instance or null
   * @throws RuntimeException if the container has no hostname
   */
  public synchronized NodeInstance getExistingNodeInstance(Container container) {
    return nodemap.get(RoleHistoryUtils.hostnameOf(container));
  }

  /**
   * Perform any pre-allocation operations on the list of allocated containers
   * based on knowledge of system state. 
   * Currently this places requested hosts ahead of unrequested ones.
   * @param allocatedContainers list of allocated containers
   * @return list of containers potentially reordered
   */
  public synchronized List<Container> prepareAllocationList(List<Container> allocatedContainers) {

    //partition into requested and unrequested
    List<Container> requested =
      new ArrayList<>(allocatedContainers.size());
    List<Container> unrequested =
      new ArrayList<>(allocatedContainers.size());
    outstandingRequests.partitionRequests(this, allocatedContainers, requested, unrequested);

    //give the unrequested ones lower priority
    requested.addAll(unrequested);
    return requested;
  }

  /**
   * A container has been allocated on a node -update the data structures
   * @param container container
   * @param desiredCount desired #of instances
   * @param actualCount current count of instances
   * @return The allocation outcome
   */
  public synchronized ContainerAllocationResults onContainerAllocated(Container container,
      long desiredCount,
      long actualCount) {
    int role = ContainerPriority.extractRole(container);

    String hostname = RoleHistoryUtils.hostnameOf(container);
    List<NodeInstance> nodeInstances = listRecentNodesForRoleId(role);
    ContainerAllocationResults outcome =
        outstandingRequests.onContainerAllocated(role, hostname, container);
    if (desiredCount <= actualCount) {
      // all outstanding requests have been satisfied
      // clear all the lists, so returning nodes to the available set
      List<NodeInstance> hosts = outstandingRequests.resetOutstandingRequests(role);
      if (!hosts.isEmpty()) {
        //add the list
        log.info("Adding {} hosts for role {}", hosts.size(), role);
        nodeInstances.addAll(hosts);
        sortRecentNodeList(role);
      }
    }
    return outcome;
  }

  /**
   * A container has been assigned to a role instance on a node -update the data structures
   * @param container container
   */
  public void onContainerAssigned(Container container) {
    NodeInstance node = getOrCreateNodeInstance(container);
    NodeEntry nodeEntry = node.getOrCreate(container);
    nodeEntry.onStarting();
    log.debug("Node {} has updated NodeEntry {}", node, nodeEntry);
  }

  /**
   * Event: a container start has been submitted
   * @param container container being started
   * @param instance instance bound to the container
   */
  public void onContainerStartSubmitted(Container container,
                                        RoleInstance instance) {
    // no actions here
  }

  /**
   * Container start event
   * @param container container that just started
   */
  public void onContainerStarted(Container container) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    nodeEntry.onStartCompleted();
    touch();
  }

  /**
   * A container failed to start: update the node entry state
   * and return the container to the queue
   * @param container container that failed
   * @return true if the node was queued
   */
  public boolean onNodeManagerContainerStartFailed(Container container) {
    return markContainerFinished(container, false, true, ContainerOutcome.Failed);
  }

  /**
   * Does the RoleHistory have enough information about the YARN cluster
   * to start placing AA requests? That is: has it the node map and
   * any label information needed?
   * @return true if the caller can start requesting AA nodes
   */
  public boolean canPlaceAANodes() {
    return nodeUpdateReceived.get();
  }

  /**
   * Get the last time the nodes were updated from YARN
   * @return the update time or zero if never updated.
   */
  public long getNodesUpdatedTime() {
    return nodesUpdatedTime.get();
  }

  /**
   * Update failedNodes and nodemap based on the node state
   *
   * @param updatedNodes list of updated nodes
   * @return true if a review should be triggered.
   */
  public synchronized boolean onNodesUpdated(List<NodeReport> updatedNodes) {
    log.debug("Updating {} nodes", updatedNodes.size());
    nodesUpdatedTime.set(now());
    nodeUpdateReceived.set(true);
    int printed = 0;
    boolean triggerReview = false;
    for (NodeReport updatedNode : updatedNodes) {
      String hostname = updatedNode.getNodeId() == null
          ? ""
          : updatedNode.getNodeId().getHost();
      NodeState nodeState = updatedNode.getNodeState();
      if (hostname.isEmpty() || nodeState == null) {
        log.warn("Ignoring incomplete update");
        continue;
      }
      if (log.isDebugEnabled() && printed++ < 10) {
        // log the first few, but avoid overloading the logs for a full cluster
        // update
        log.debug("Node \"{}\" is in state {}", hostname, nodeState);
      }
      // update the node; this also creates an instance if needed
      boolean updated = nodemap.updateNode(hostname, updatedNode);
      triggerReview |= updated;
    }
    return triggerReview;
  }

  /**
   * A container release request was issued
   * @param container container submitted
   */
  public void onContainerReleaseSubmitted(Container container) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    nodeEntry.release();
  }

  /**
   * App state notified of a container completed 
   * @param container completed container
   * @return true if the node was queued
   */
  public boolean onReleaseCompleted(Container container) {
    return markContainerFinished(container, true, false, ContainerOutcome.Failed);
  }

  /**
   * App state notified of a container completed -but as
   * it wasn't being released it is marked as failed
   *
   * @param container completed container
   * @param shortLived was the container short lived?
   * @param outcome
   * @return true if the node is considered available for work
   */
  public boolean onFailedContainer(Container container,
      boolean shortLived,
      ContainerOutcome outcome) {
    return markContainerFinished(container, false, shortLived, outcome);
  }

  /**
   * Mark a container finished; if it was released then that is treated
   * differently. history is {@code touch()}-ed
   *
   *
   * @param container completed container
   * @param wasReleased was the container released?
   * @param shortLived was the container short lived?
   * @param outcome
   * @return true if the node was queued
   */
  protected synchronized boolean markContainerFinished(Container container,
      boolean wasReleased,
      boolean shortLived,
      ContainerOutcome outcome) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    log.info("Finished container for node {}, released={}, shortlived={}",
        nodeEntry.rolePriority, wasReleased, shortLived);
    boolean available;
    if (shortLived) {
      nodeEntry.onStartFailed();
      available = false;
    } else {
      available = nodeEntry.containerCompleted(wasReleased, outcome);
      maybeQueueNodeForWork(container, nodeEntry, available);
    }
    touch();
    return available;
  }

  /**
   * If the node is marked as available; queue it for assignments.
   * Unsynced: requires caller to be in a sync block.
   * @param container completed container
   * @param nodeEntry node
   * @param available available flag
   * @return true if the node was queued
   */
  private boolean maybeQueueNodeForWork(Container container,
                                        NodeEntry nodeEntry,
                                        boolean available) {
    if (available) {
      //node is free
      nodeEntry.setLastUsed(now());
      NodeInstance ni = getOrCreateNodeInstance(container);
      int roleId = ContainerPriority.extractRole(container);
      log.debug("Node {} is now available for role id {}", ni, roleId);
      listRecentNodesForRoleId(roleId).addFirst(ni);
    }
    return available;
  }

  /**
   * Print the history to the log. This is for testing and diagnostics 
   */
  public synchronized void dump() {
    for (ProviderRole role : providerRoles) {
      log.info(role.toString());
      List<NodeInstance> instances = listRecentNodesForRoleId(role.id);
      log.info("  available: " + instances.size()
               + " " + SliderUtils.joinWithInnerSeparator(" ", instances));
    }

    log.info("Nodes in Cluster: {}", getClusterSize());
    for (NodeInstance node : nodemap.values()) {
      log.info(node.toFullString());
    }
  }

  /**
   * Build the mapping entry for persisting to the role history
   * @return a mapping object
   */
  public synchronized Map<CharSequence, Integer> buildMappingForHistoryFile() {
    Map<CharSequence, Integer> mapping = new HashMap<>(getRoleSize());
    for (ProviderRole role : providerRoles) {
      mapping.put(role.name, role.id);
    }
    return mapping;
  }

  /**
   * Get a clone of the available list
   * @param role role index
   * @return a clone of the list
   */
  @VisibleForTesting
  public List<NodeInstance> cloneRecentNodeList(int role) {
    return new LinkedList<>(listRecentNodesForRoleId(role));
  }

  /**
   * Get a snapshot of the outstanding placed request list
   * @return a list of the requests outstanding at the time of requesting
   */
  @VisibleForTesting
  public List<OutstandingRequest> listPlacedRequests() {
    return outstandingRequests.listPlacedRequests();
  }

  /**
   * Get a snapshot of the outstanding placed request list
   * @return a list of the requests outstanding at the time of requesting
   */
  @VisibleForTesting
  public List<OutstandingRequest> listOpenRequests() {
    return outstandingRequests.listOpenRequests();
  }

  /**
   * Escalate operation as triggered by external timer.
   * @return a (usually empty) list of cancel/request operations.
   */
  public synchronized List<AbstractRMOperation> escalateOutstandingRequests() {
    return outstandingRequests.escalateOutstandingRequests(now());
  }
  /**
   * Escalate operation as triggered by external timer.
   * @return a (usually empty) list of cancel/request operations.
   */
  public List<AbstractRMOperation> cancelOutstandingAARequests() {
    return outstandingRequests.cancelOutstandingAARequests();
  }

  /**
   * Cancel a number of outstanding requests for a role -that is, not
   * actual containers, just requests for new ones.
   * @param role role
   * @param toCancel number to cancel
   * @return a list of cancellable operations.
   */
  public List<AbstractRMOperation> cancelRequestsForRole(RoleStatus role, int toCancel) {
    return role.isAntiAffinePlacement() ?
        cancelRequestsForAARole(role, toCancel)
        : cancelRequestsForSimpleRole(role, toCancel);
  }

  /**
   * Build the list of requests to cancel from the outstanding list.
   * @param role role
   * @param toCancel number to cancel
   * @return a list of cancellable operations.
   */
  private synchronized List<AbstractRMOperation> cancelRequestsForSimpleRole(RoleStatus role, int toCancel) {
    Preconditions.checkArgument(toCancel > 0,
        "trying to cancel invalid number of requests: " + toCancel);
    List<AbstractRMOperation> results = new ArrayList<>(toCancel);
    // first scan through the unplaced request list to find all of a role
    int roleId = role.getKey();
    List<OutstandingRequest> requests =
        outstandingRequests.extractOpenRequestsForRole(roleId, toCancel);

    // are there any left?
    int remaining = toCancel - requests.size();
    // ask for some placed nodes
    requests.addAll(outstandingRequests.extractPlacedRequestsForRole(roleId, remaining));

    // build cancellations
    for (OutstandingRequest request : requests) {
      results.add(request.createCancelOperation());
    }
    return results;
  }

  /**
   * Build the list of requests to cancel for an AA role. This reduces the number
   * of outstanding pending requests first, then cancels any active request,
   * before finally asking for any placed containers
   * @param role role
   * @param toCancel number to cancel
   * @return a list of cancellable operations.
   */
  private synchronized List<AbstractRMOperation> cancelRequestsForAARole(RoleStatus role, int toCancel) {
    List<AbstractRMOperation> results = new ArrayList<>(toCancel);
    int roleId = role.getKey();
    List<OutstandingRequest> requests = new ArrayList<>(toCancel);
    // there may be pending requests which can be cancelled here
    long pending = role.getPendingAntiAffineRequests();
    if (pending > 0) {
      // there are some pending ones which can be cancelled first
      long pendingToCancel = Math.min(pending, toCancel);
      log.info("Cancelling {} pending AA allocations, leaving {}", toCancel,
          pendingToCancel);
      role.setPendingAntiAffineRequests(pending - pendingToCancel);
      toCancel -= pendingToCancel;
    }
    if (toCancel > 0 && role.isAARequestOutstanding()) {
      // not enough
      log.info("Cancelling current AA request");
      // find the single entry which may be running
      requests = outstandingRequests.extractOpenRequestsForRole(roleId, toCancel);
      role.cancelOutstandingAARequest();
      toCancel--;
    }

    // ask for some excess nodes
    if (toCancel > 0) {
      requests.addAll(outstandingRequests.extractPlacedRequestsForRole(roleId, toCancel));
    }

    // build cancellations
    for (OutstandingRequest request : requests) {
      results.add(request.createCancelOperation());
    }
    return results;
  }

}
