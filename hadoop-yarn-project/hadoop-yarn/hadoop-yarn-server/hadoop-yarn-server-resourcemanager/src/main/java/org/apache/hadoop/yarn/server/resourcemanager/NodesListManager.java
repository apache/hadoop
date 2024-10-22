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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.HostsFileReader.HostDetails;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent.RMAppNodeUpdateType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeDecommissioningEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import org.apache.hadoop.classification.VisibleForTesting;

@SuppressWarnings("unchecked")
public class NodesListManager extends CompositeService implements
    EventHandler<NodesListManagerEvent> {

  private static final Logger LOG =
      LoggerFactory.getLogger(NodesListManager.class);

  private HostsFileReader hostsReader;
  private Configuration conf;
  private final RMContext rmContext;

  // Default decommissioning timeout value in seconds.
  // Negative value indicates no timeout. 0 means immediate.
  private int defaultDecTimeoutSecs =
      YarnConfiguration.DEFAULT_RM_NODE_GRACEFUL_DECOMMISSION_TIMEOUT;

  private String includesFile;
  private String excludesFile;

  private Resolver resolver;
  private Timer removalTimer;
  private int nodeRemovalCheckInterval;
  private Set<RMNode> gracefulDecommissionableNodes;
  private boolean enableNodeUntrackedWithoutIncludePath;

  public NodesListManager(RMContext rmContext) {
    super(NodesListManager.class.getName());
    this.rmContext = rmContext;
    this.gracefulDecommissionableNodes = ConcurrentHashMap.newKeySet();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.conf = conf;

    int nodeIpCacheTimeout = conf.getInt(
        YarnConfiguration.RM_NODE_IP_CACHE_EXPIRY_INTERVAL_SECS,
        YarnConfiguration.DEFAULT_RM_NODE_IP_CACHE_EXPIRY_INTERVAL_SECS);
    if (nodeIpCacheTimeout <= 0) {
      resolver = new DirectResolver();
    } else {
      resolver =
          new CachedResolver(SystemClock.getInstance(), nodeIpCacheTimeout);
      addIfService(resolver);
    }

    // Read the hosts/exclude files to restrict access to the RM
    try {
      this.includesFile = conf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
          YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH);
      this.excludesFile = conf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
          YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH);
      this.hostsReader =
          createHostsFileReader(this.includesFile, this.excludesFile);
      LOG.debug("Calling setDecommissionedNMs from serviceInit");
      setDecommissionedNMs();
      printConfiguredHosts(false);
    } catch (YarnException ex) {
      disableHostsFileReader(ex);
    } catch (IOException ioe) {
      disableHostsFileReader(ioe);
    }

    enableNodeUntrackedWithoutIncludePath = conf.getBoolean(
        YarnConfiguration.RM_ENABLE_NODE_UNTRACKED_WITHOUT_INCLUDE_PATH,
        YarnConfiguration.DEFAULT_RM_ENABLE_NODE_UNTRACKED_WITHOUT_INCLUDE_PATH);
    final Set<String> untrackedSelectiveStatesToRemove = Arrays.stream(conf.getStrings(
        YarnConfiguration.RM_NODEMANAGER_UNTRACKED_NODE_SELECTIVE_STATES_TO_REMOVE,
        YarnConfiguration.DEFAULT_RM_NODEMANAGER_UNTRACKED_NODE_SELECTIVE_STATES_TO_REMOVE))
            .collect(Collectors.toSet());
    final int nodeRemovalTimeout =
        conf.getInt(
            YarnConfiguration.RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC,
            YarnConfiguration.
                DEFAULT_RM_NODEMANAGER_UNTRACKED_REMOVAL_TIMEOUT_MSEC);
    nodeRemovalCheckInterval = (Math.min(nodeRemovalTimeout/2,
        600000));
    removalTimer = new Timer("Node Removal Timer");

    removalTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        long now = Time.monotonicNow();
        for (Map.Entry<NodeId, RMNode> entry :
            rmContext.getInactiveRMNodes().entrySet()) {
          NodeId nodeId = entry.getKey();
          RMNode rmNode = entry.getValue();
          if (isUntrackedNode(rmNode.getHostName())) {
            if(CollectionUtils.isNotEmpty(untrackedSelectiveStatesToRemove) &&
                !untrackedSelectiveStatesToRemove.contains(rmNode.getState().toString())) {
              LOG.warn("Untracked node {}, with node state {} is not part of " +
                  "node-removal-untracked.node-selective-states-to-remove config",
                  rmNode.getHostName(), rmNode.getState().toString());
              continue;
            }
            if (rmNode.getUntrackedTimeStamp() == 0) {
              rmNode.setUntrackedTimeStamp(now);
            } else
              if (now - rmNode.getUntrackedTimeStamp() >
                  nodeRemovalTimeout) {
                RMNode result = rmContext.getInactiveRMNodes().remove(nodeId);
                if (result != null) {
                  decrInactiveNMMetrics(rmNode);
                  LOG.info("Removed " +result.getState().toString() + " node "
                      + result.getHostName() + " from inactive nodes list");
                }
              }
          } else {
            rmNode.setUntrackedTimeStamp(0);
          }
        }
      }
    }, nodeRemovalCheckInterval, nodeRemovalCheckInterval);

    super.serviceInit(conf);
  }

  private void decrInactiveNMMetrics(RMNode rmNode) {
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    switch (rmNode.getState()) {
    case SHUTDOWN:
      clusterMetrics.decrNumShutdownNMs();
      break;
    case DECOMMISSIONED:
      clusterMetrics.decrDecommisionedNMs();
      break;
    case LOST:
      clusterMetrics.decrNumLostNMs();
      break;
    case REBOOTED:
      clusterMetrics.decrNumRebootedNMs();
      break;
    default:
      LOG.debug("Unexpected node state");
    }
  }

  @Override
  public void serviceStop() {
    removalTimer.cancel();
  }

  private void printConfiguredHosts(boolean graceful) {
    if (!LOG.isDebugEnabled()) {
      return;
    }

    LOG.debug("hostsReader: in=" +
        conf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
        YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH) + " out=" +
        conf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
            YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH));

    HostDetails hostDetails;
    if (graceful) {
      hostDetails = hostsReader.getLazyLoadedHostDetails();
    } else {
      hostDetails = hostsReader.getHostDetails();
    }
    for (String include : hostDetails.getIncludedHosts()) {
      LOG.debug("include: " + include);
    }
    for (String exclude : hostDetails.getExcludedHosts()) {
      LOG.debug("exclude: " + exclude);
    }
  }

  public void refreshNodes(Configuration yarnConf)
      throws IOException, YarnException {
    try {
      refreshNodes(yarnConf, false);
    } catch (YarnException | IOException ex) {
      disableHostsFileReader(ex);
    }
  }

  public void refreshNodesForActiveRMTransition(Configuration yarnConf)
      throws IOException, YarnException {
    refreshHostsReader(yarnConf, false, null);
    /*
     * The call to refreshHostsReader() will read the excludeFile and handle
     * decommission for nodes which are in this.rmContext.getRMNodes(). When an
     * active RM newly started, the serviceInit() method of this
     * NodesListManager Service would have been called. There is a call to
     * setDecommissionedNMs which will make an artificial state change for those
     * excluded nodes. This will help RM to give a proper list about
     * decommissioned nodes. But in case of an active standby RM HA, the standby
     * RM is already started and the serviceInit() of this Service been already
     * happened. Later on the exclude file got added with some decommissioning
     * nodes. When this standby become active, we refresh the nodes and as part
     * of that reread the exclude file. But by then we don't have any entries in
     * this.rmContext.getRMNodes() and so not really able to make any state
     * transition for any of the nodes. Later, this RM will never give those
     * nodes in decommissioned list. At the same time those NMs will get
     * shutdown as the RM already had reinit the excluded nodes list. We are
     * calling this method whenever an RM becomes active and so calling
     * setDecommissionedNMs here also which will add those excluded nodes into
     * rmContext.getInactiveRMNodes() and will do a decommission state
     * transition.
     */
    LOG.debug("Calling setDecommissionedNMs as part of RM becoming active.");
    setDecommissionedNMs();
  }

  public void refreshNodes(Configuration yarnConf, boolean graceful)
      throws IOException, YarnException {
    refreshHostsReader(yarnConf, graceful, null);
  }

  private void refreshHostsReader(
      Configuration yarnConf, boolean graceful, Integer timeout)
          throws IOException, YarnException {
    // resolve the default timeout to the decommission timeout that is
    // configured at this moment
    if (null == timeout) {
      timeout = readDecommissioningTimeout(yarnConf);
    }
    if (null == yarnConf) {
      yarnConf = new YarnConfiguration();
    }
    includesFile =
        yarnConf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
            YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH);
    excludesFile =
        yarnConf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
            YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH);
    LOG.info("refreshNodes excludesFile " + excludesFile);

    if (graceful) {
      // update hosts, but don't make it visible just yet
      hostsReader.lazyRefresh(includesFile, excludesFile);
    } else {
      hostsReader.refresh(includesFile, excludesFile);
    }

    printConfiguredHosts(graceful);

    LOG.info("hostsReader include:{" +
        StringUtils.join(",", hostsReader.getHosts()) +
        "} exclude:{" +
        StringUtils.join(",", hostsReader.getExcludedHosts()) + "}");

    handleExcludeNodeList(graceful, timeout);
    markUnregisteredNodesAsLost(yarnConf);
  }

  private void setDecommissionedNMs() {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    for (final String host : excludeList) {
      NodeId nodeId = createUnknownNodeId(host);
      if (!rmContext.getInactiveRMNodes().containsKey(nodeId)) {
        RMNodeImpl rmNode = new RMNodeImpl(nodeId,
            rmContext, host, -1, -1, new UnknownNode(host),
            Resource.newInstance(0, 0), "unknown");
        rmContext.getInactiveRMNodes().put(nodeId, rmNode);
        rmNode.handle(new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
      }
    }
    // In case of RM switchover, there might be NMs already added into the
    // rmContext#InactiveRMNodes.
    // This got added while this RM started as standby RM.
    // If an NM in this inactive nodes list, is currently not excluded as per
    // excluded file, we have to remove that node from InactiveRMNodes.
    Iterator<Entry<NodeId, RMNode>> inactiveNodes = rmContext
        .getInactiveRMNodes().entrySet().iterator();
    while (inactiveNodes.hasNext()) {
      Entry<NodeId, RMNode> inactiveNode = inactiveNodes.next();
      if (!excludeList.contains(inactiveNode.getKey().getHost())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removing the node " + inactiveNode.getKey().getHost()
              + " from inactiveRMNodes as it is no longer part of excluded nodes");
        }
        inactiveNodes.remove();
      }
    }
  }

  // Handle excluded nodes based on following rules:
  // Recommission DECOMMISSIONED or DECOMMISSIONING nodes no longer excluded;
  // Gracefully decommission excluded nodes that are not already
  // DECOMMISSIONED nor DECOMMISSIONING; Take no action for excluded nodes
  // that are already DECOMMISSIONED or DECOMMISSIONING.
  private void handleExcludeNodeList(boolean graceful, int timeout) {
    // DECOMMISSIONED/DECOMMISSIONING nodes need to be re-commissioned.
    List<RMNode> nodesToRecom = new ArrayList<RMNode>();

    // Nodes need to be decommissioned (graceful or forceful);
    List<RMNode> nodesToDecom = new ArrayList<RMNode>();

    HostDetails hostDetails;
    gracefulDecommissionableNodes.clear();
    if (graceful) {
      hostDetails = hostsReader.getLazyLoadedHostDetails();
    } else {
      hostDetails = hostsReader.getHostDetails();
    }

    Set<String> includes = hostDetails.getIncludedHosts();
    Map<String, Integer> excludes = hostDetails.getExcludedMap();

    for (RMNode n : this.rmContext.getRMNodes().values()) {
      NodeState s = n.getState();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking possible decommission/recommission for the NM "
            + n.getHostName() + ". Current state " + s);
      }
      // An invalid node (either due to explicit exclude or not include)
      // should be excluded.
      boolean isExcluded = !isValidNode(
          n.getHostName(), includes, excludes.keySet());
      String nodeStr = "node " + n.getNodeID() + " with state " + s;
      if (!isExcluded) {
        // Note that no action is needed for DECOMMISSIONED node.
        if (s == NodeState.DECOMMISSIONING) {
          LOG.info("Recommission " + nodeStr);
          nodesToRecom.add(n);
        }
        // Otherwise no-action needed.
      } else {
        // exclude is true.
        if (graceful) {
          // Use per node timeout if exist otherwise the request timeout.
          Integer timeoutToUse = (excludes.get(n.getHostName()) != null)?
              excludes.get(n.getHostName()) : timeout;
          if (s != NodeState.DECOMMISSIONED &&
              s != NodeState.DECOMMISSIONING) {
            LOG.info("Gracefully decommission " + nodeStr);
            nodesToDecom.add(n);
            gracefulDecommissionableNodes.add(n);
          } else if (s == NodeState.DECOMMISSIONING &&
                     !Objects.equals(n.getDecommissioningTimeout(),
                         timeoutToUse)) {
            LOG.info("Update " + nodeStr + " timeout to be " + timeoutToUse);
            nodesToDecom.add(n);
            gracefulDecommissionableNodes.add(n);
          } else {
            LOG.info("No action for " + nodeStr);
          }
        } else {
          if (s != NodeState.DECOMMISSIONED) {
            LOG.info("Forcefully decommission " + nodeStr);
            nodesToDecom.add(n);
          }
        }
      }
    }

    if (graceful) {
      hostsReader.finishRefresh();
    }

    for (RMNode n : nodesToRecom) {
      RMNodeEvent e = new RMNodeEvent(
          n.getNodeID(), RMNodeEventType.RECOMMISSION);
      this.rmContext.getDispatcher().getEventHandler().handle(e);
    }

    for (RMNode n : nodesToDecom) {
      RMNodeEvent e;
      if (graceful) {
        Integer timeoutToUse = (excludes.get(n.getHostName()) != null)?
            excludes.get(n.getHostName()) : timeout;
        e = new RMNodeDecommissioningEvent(n.getNodeID(), timeoutToUse);
      } else {
        RMNodeEventType eventType = isUntrackedNode(n.getHostName())?
            RMNodeEventType.SHUTDOWN : RMNodeEventType.DECOMMISSION;
        e = new RMNodeEvent(n.getNodeID(), eventType);
      }
      this.rmContext.getDispatcher().getEventHandler().handle(e);
    }

    updateInactiveNodes();
  }

  /**
   * Marks the unregistered nodes as LOST
   * if the feature is enabled via a configuration flag.
   *
   * This method finds nodes that are present in the include list but are not
   * registered with the ResourceManager. Such nodes are then marked as LOST.
   *
   * The steps are as follows:
   * 1. Retrieve all hostnames of registered nodes from RM.
   * 2. Identify the nodes present in the include list but are not registered
   * 3. Remove nodes from the exclude list
   * 4. Dispatch LOST events for filtered nodes to mark them as LOST.
   *
   * @param yarnConf Configuration object that holds the YARN configurations.
   */
  private void markUnregisteredNodesAsLost(Configuration yarnConf) {
    // Check if tracking unregistered nodes is enabled in the configuration
    if (!yarnConf.getBoolean(YarnConfiguration.ENABLE_TRACKING_FOR_UNREGISTERED_NODES,
        YarnConfiguration.DEFAULT_ENABLE_TRACKING_FOR_UNREGISTERED_NODES)) {
      LOG.debug("Unregistered node tracking is disabled. " +
          "Skipping marking unregistered nodes as LOST.");
      return;
    }

    // Set to store all registered hostnames from both active and inactive lists
    Set<String> registeredHostNames = gatherRegisteredHostNames();
    // Event handler to dispatch LOST events
    EventHandler eventHandler = this.rmContext.getDispatcher().getEventHandler();

    // Identify nodes that are in the include list but are not registered
    // and are not in the exclude list
    List<String> nodesToMarkLost = new ArrayList<>();
    HostDetails hostDetails = hostsReader.getHostDetails();
    Set<String> includes = hostDetails.getIncludedHosts();
    Set<String> excludes = hostDetails.getExcludedHosts();

    for (String includedNode : includes) {
      if (!registeredHostNames.contains(includedNode) && !excludes.contains(includedNode)) {
        LOG.info("Lost node: {}", includedNode);
        nodesToMarkLost.add(includedNode);
      }
    }

    // Dispatch LOST events for the identified lost nodes
    for (String lostNode : nodesToMarkLost) {
      dispatchLostEvent(eventHandler, lostNode);
    }

    // Log successful completion of marking unregistered nodes as LOST
    LOG.info("Successfully marked unregistered nodes as LOST");
  }

  /**
   * Gathers all registered hostnames from both active and inactive RMNodes.
   *
   * @return A set of registered hostnames.
   */
  private Set<String> gatherRegisteredHostNames() {
    Set<String> registeredHostNames = new HashSet<>();
    LOG.info("Getting all the registered hostnames");

    // Gather all registered nodes (active) from RM into the set
    for (RMNode node : this.rmContext.getRMNodes().values()) {
      registeredHostNames.add(node.getHostName());
    }

    // Gather all inactive nodes from RM into the set
    for (RMNode node : this.rmContext.getInactiveRMNodes().values()) {
      registeredHostNames.add(node.getHostName());
    }

    return registeredHostNames;
  }

  /**
   * Dispatches a LOST event for a specified lost node.
   *
   * @param eventHandler The EventHandler used to dispatch the LOST event.
   * @param lostNode     The hostname of the lost node for which the event is
   *                     being dispatched.
   */
  private void dispatchLostEvent(EventHandler eventHandler, String lostNode) {
    // Generate a NodeId for the lost node with a special port -2
    NodeId nodeId = createLostNodeId(lostNode);
    RMNodeEvent lostEvent = new RMNodeEvent(nodeId, RMNodeEventType.EXPIRE);
    RMNodeImpl rmNode = new RMNodeImpl(nodeId, this.rmContext, lostNode, -2, -2,
        new UnknownNode(lostNode), Resource.newInstance(0, 0), "unknown");

    try {
      // Dispatch the LOST event to signal the node is no longer active
      eventHandler.handle(lostEvent);

      // After successful dispatch, update the node status in RMContext
      // Set the node's timestamp for when it became untracked
      rmNode.setUntrackedTimeStamp(Time.monotonicNow());

      // Add the node to the active and inactive node maps in RMContext
      this.rmContext.getRMNodes().put(nodeId, rmNode);
      this.rmContext.getInactiveRMNodes().put(nodeId, rmNode);

      LOG.info("Successfully dispatched LOST event and deactivated node: {}, Node ID: {}",
          lostNode, nodeId);
    } catch (Exception e) {
      // Log any exception encountered during event dispatch
      LOG.error("Error dispatching LOST event for node: {}, Node ID: {} - {}",
          lostNode, nodeId, e.getMessage());
    }
  }

  @VisibleForTesting
  public int getNodeRemovalCheckInterval() {
    return nodeRemovalCheckInterval;
  }

  @VisibleForTesting
  public void setNodeRemovalCheckInterval(int interval) {
    this.nodeRemovalCheckInterval = interval;
  }

  @VisibleForTesting
  public Resolver getResolver() {
    return resolver;
  }

  @VisibleForTesting
  public interface Resolver {
    // try to resolve hostName to IP address, fallback to hostName if failed
    String resolve(String hostName);
  }

  @VisibleForTesting
  public static class DirectResolver implements Resolver {
    @Override
    public String resolve(String hostName) {
      return NetUtils.normalizeHostName(hostName);
    }
  }

  @VisibleForTesting
  public static class CachedResolver extends AbstractService
      implements Resolver {
    private static class CacheEntry {
      public String ip;
      public long resolveTime;
      public CacheEntry(String ip, long resolveTime) {
        this.ip = ip;
        this.resolveTime = resolveTime;
      }
    }
    private Map<String, CacheEntry> cache =
        new ConcurrentHashMap<String, CacheEntry>();
    private int expiryIntervalMs;
    private int checkIntervalMs;
    private final Clock clock;
    private Timer checkingTimer;
    private TimerTask expireChecker = new ExpireChecker();

    public CachedResolver(Clock clock, int expiryIntervalSecs) {
      super("NodesListManager.CachedResolver");
      this.clock = clock;
      this.expiryIntervalMs = expiryIntervalSecs * 1000;
      checkIntervalMs = expiryIntervalMs/3;
      checkingTimer = new Timer(
          "Timer-NodesListManager.CachedResolver.ExpireChecker", true);
    }

    @Override
    protected void serviceStart() throws Exception {
      checkingTimer.scheduleAtFixedRate(
          expireChecker, checkIntervalMs, checkIntervalMs);
      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
      checkingTimer.cancel();
      super.serviceStop();
    }

    @VisibleForTesting
    public void addToCache(String hostName, String ip) {
      cache.put(hostName, new CacheEntry(ip, clock.getTime()));
    }

    public void removeFromCache(String hostName) {
      cache.remove(hostName);
    }

    private String reload(String hostName) {
      String ip = NetUtils.normalizeHostName(hostName);
      addToCache(hostName, ip);
      return ip;
    }

    @Override
    public String resolve(String hostName) {
      CacheEntry e = cache.get(hostName);
      if (e != null) {
        return e.ip;
      }
      return reload(hostName);
    }

    @VisibleForTesting
    public TimerTask getExpireChecker() {
      return expireChecker;
    }

    private class ExpireChecker extends TimerTask {
      @Override
      public void run() {
        long currentTime = clock.getTime();
        Iterator<Map.Entry<String, CacheEntry>> iterator =
            cache.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<String, CacheEntry> entry = iterator.next();
          if (currentTime >
              entry.getValue().resolveTime +
                  CachedResolver.this.expiryIntervalMs) {
            iterator.remove();
            if (LOG.isDebugEnabled()) {
              LOG.debug("[" + entry.getKey() + ":" + entry.getValue().ip +
                  "] Expired after " +
                  CachedResolver.this.expiryIntervalMs / 1000 + " secs");
            }
          }
        }
      }
    }
  }

  public boolean isValidNode(String hostName) {
    HostDetails hostDetails = hostsReader.getHostDetails();
    return isValidNode(hostName, hostDetails.getIncludedHosts(),
        hostDetails.getExcludedHosts());
  }

  boolean isGracefullyDecommissionableNode(RMNode node) {
    return gracefulDecommissionableNodes.contains(node);
  }

  private boolean isValidNode(
      String hostName, Set<String> hostsList, Set<String> excludeList) {
    String ip = resolver.resolve(hostName);
    return (hostsList.isEmpty() || hostsList.contains(hostName) || hostsList
        .contains(ip))
        && !(excludeList.contains(hostName) || excludeList.contains(ip));
  }

  private void sendRMAppNodeUpdateEventToNonFinalizedApps(
      RMNode eventNode, RMAppNodeUpdateType appNodeUpdateType) {
    for(RMApp app : rmContext.getRMApps().values()) {
      if (!app.isAppFinalStateStored()) {
        app.handle(new RMAppNodeUpdateEvent(app.getApplicationId(), eventNode,
            appNodeUpdateType));
      }
    }
  }

  @Override
  public void handle(NodesListManagerEvent event) {
    RMNode eventNode = event.getNode();
    switch (event.getType()) {
    case NODE_UNUSABLE:
      LOG.debug("{} reported unusable", eventNode);
      sendRMAppNodeUpdateEventToNonFinalizedApps(eventNode,
          RMAppNodeUpdateType.NODE_UNUSABLE);
      break;
    case NODE_USABLE:
      LOG.debug("{} reported usable", eventNode);
      sendRMAppNodeUpdateEventToNonFinalizedApps(eventNode,
          RMAppNodeUpdateType.NODE_USABLE);
      break;
    case NODE_DECOMMISSIONING:
      LOG.debug("{} reported decommissioning", eventNode);
      sendRMAppNodeUpdateEventToNonFinalizedApps(
          eventNode, RMAppNodeUpdateType.NODE_DECOMMISSIONING);
      break;

    default:
      LOG.error("Ignoring invalid eventtype " + event.getType());
    }
    // remove the cache of normalized hostname if enabled
    if (resolver instanceof CachedResolver) {
      ((CachedResolver)resolver).removeFromCache(
          eventNode.getNodeID().getHost());
    }
  }

  private void disableHostsFileReader(Exception ex) {
    LOG.warn("Failed to init hostsReader, disabling", ex);
    try {
      this.includesFile =
          conf.get(YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH);
      this.excludesFile =
          conf.get(YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH);
      this.hostsReader =
          createHostsFileReader(this.includesFile, this.excludesFile);
      setDecommissionedNMs();
    } catch (IOException ioe2) {
      // Should *never* happen
      this.hostsReader = null;
      throw new YarnRuntimeException(ioe2);
    } catch (YarnException e) {
      // Should *never* happen
      this.hostsReader = null;
      throw new YarnRuntimeException(e);
    }
  }

  @VisibleForTesting
  public HostsFileReader getHostsReader() {
    return this.hostsReader;
  }

  private HostsFileReader createHostsFileReader(String includesFile,
      String excludesFile) throws IOException, YarnException {
    HostsFileReader hostsReader =
        new HostsFileReader(includesFile,
            (includesFile == null || includesFile.isEmpty()) ? null
                : this.rmContext.getConfigurationProvider()
                    .getConfigurationInputStream(this.conf, includesFile),
            excludesFile,
            (excludesFile == null || excludesFile.isEmpty()) ? null
                : this.rmContext.getConfigurationProvider()
                    .getConfigurationInputStream(this.conf, excludesFile));
    return hostsReader;
  }

  private void updateInactiveNodes() {
    long now = Time.monotonicNow();
    for(Entry<NodeId, RMNode> entry :
        rmContext.getInactiveRMNodes().entrySet()) {
      NodeId nodeId = entry.getKey();
      RMNode rmNode = entry.getValue();
      if (isUntrackedNode(nodeId.getHost()) &&
          rmNode.getUntrackedTimeStamp() == 0) {
        rmNode.setUntrackedTimeStamp(now);
      }
    }
  }

  public boolean isUntrackedNode(String hostName) {
    String ip = resolver.resolve(hostName);

    HostDetails hostDetails = hostsReader.getHostDetails();
    Set<String> hostsList = hostDetails.getIncludedHosts();
    Set<String> excludeList = hostDetails.getExcludedHosts();

    return (!hostsList.isEmpty() || (enableNodeUntrackedWithoutIncludePath
          && (hostDetails.getIncludesFile() == null
              || hostDetails.getIncludesFile().isEmpty())))
        && !hostsList.contains(hostName)
        && !hostsList.contains(ip) && !excludeList.contains(hostName)
        && !excludeList.contains(ip);
  }

  /**
   * Refresh the nodes gracefully.
   *
   * @param yarnConf yarn configuration.
   * @param timeout decommission timeout, null means default timeout.
   * @throws IOException io error occur.
   * @throws YarnException exceptions from yarn servers.
   */
  public void refreshNodesGracefully(Configuration yarnConf, Integer timeout)
      throws IOException, YarnException {
    refreshHostsReader(yarnConf, true, timeout);
  }

  /**
   * It checks for any nodes in decommissioning state
   *
   * @return decommissioning nodes
   */
  public Set<NodeId> checkForDecommissioningNodes() {
    Set<NodeId> decommissioningNodes = new HashSet<NodeId>();
    for (Entry<NodeId, RMNode> entry : rmContext.getRMNodes().entrySet()) {
      if (entry.getValue().getState() == NodeState.DECOMMISSIONING) {
        decommissioningNodes.add(entry.getKey());
      }
    }
    return decommissioningNodes;
  }

  /**
   * Forcefully decommission the nodes if they are in DECOMMISSIONING state
   */
  public void refreshNodesForcefully() {
    for (Entry<NodeId, RMNode> entry : rmContext.getRMNodes().entrySet()) {
      if (entry.getValue().getState() == NodeState.DECOMMISSIONING) {
        RMNodeEventType nodeEventType =
            isUntrackedNode(entry.getKey().getHost()) ?
            RMNodeEventType.SHUTDOWN : RMNodeEventType.DECOMMISSION;
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(entry.getKey(), nodeEventType));
      }
    }
  }

  // Read possible new DECOMMISSIONING_TIMEOUT_KEY from yarn-site.xml.
  // This enables NodesListManager to pick up new value without
  // ResourceManager restart.
  private int readDecommissioningTimeout(Configuration pConf) {
    try {
      if (pConf == null) {
        pConf = new YarnConfiguration();
      }
      int configuredDefaultDecTimeoutSecs =
          pConf.getInt(YarnConfiguration.RM_NODE_GRACEFUL_DECOMMISSION_TIMEOUT,
              YarnConfiguration.DEFAULT_RM_NODE_GRACEFUL_DECOMMISSION_TIMEOUT);
      if (defaultDecTimeoutSecs != configuredDefaultDecTimeoutSecs) {
        defaultDecTimeoutSecs = configuredDefaultDecTimeoutSecs;
        LOG.info("Use new decommissioningTimeoutSecs: "
            + defaultDecTimeoutSecs);
      }
    } catch (Exception e) {
      LOG.warn("Error readDecommissioningTimeout " + e.getMessage());
    }
    return defaultDecTimeoutSecs;
  }

  /**
   * A NodeId instance needed upon startup for populating inactive nodes Map.
   * It only knows the hostname/ip and marks the port to -1 or invalid.
   *
   * @param host host name.
   * @return node id.
   */
  public static NodeId createUnknownNodeId(String host) {
    return NodeId.newInstance(host, -1);
  }

  /**
   * Creates a NodeId for a node marked as LOST.
   *
   * The NodeId combines the hostname with a special port value of -2, indicating
   * that the node is lost in the cluster.
   *
   * @param host The hostname of the lost node.
   * @return NodeId Unique identifier for the lost node, with the port set to -2.
   */
  public static NodeId createLostNodeId(String host) {
    // Create a NodeId with the given host and port -2 to signify the node is lost.
    return NodeId.newInstance(host, -2);
  }

  /**
   * A Node instance needed upon startup for populating inactive nodes Map.
   * It only knows its hostname/ip.
   */
  private static class UnknownNode implements Node {

    private String host;

    public UnknownNode(String host) {
      this.host = host;
    }

    @Override
    public String getNetworkLocation() {
      return null;
    }

    @Override
    public void setNetworkLocation(String location) {

    }

    @Override
    public String getName() {
      return host;
    }

    @Override
    public Node getParent() {
      return null;
    }

    @Override
    public void setParent(Node parent) {

    }

    @Override
    public int getLevel() {
      return 0;
    }

    @Override
    public void setLevel(int i) {

    }

    public String getHost() {
      return host;
    }

    public void setHost(String hst) {
      this.host = hst;
    }
  }
}
