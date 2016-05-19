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
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent.RMAppNodeUpdateType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

@SuppressWarnings("unchecked")
public class NodesListManager extends CompositeService implements
    EventHandler<NodesListManagerEvent> {

  private static final Log LOG = LogFactory.getLog(NodesListManager.class);

  private HostsFileReader hostsReader;
  private Configuration conf;
  private final RMContext rmContext;

  private String includesFile;
  private String excludesFile;

  private Resolver resolver;
  private Timer removalTimer;
  private int nodeRemovalCheckInterval;

  public NodesListManager(RMContext rmContext) {
    super(NodesListManager.class.getName());
    this.rmContext = rmContext;
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
      setDecomissionedNMs();
      printConfiguredHosts();
    } catch (YarnException ex) {
      disableHostsFileReader(ex);
    } catch (IOException ioe) {
      disableHostsFileReader(ioe);
    }

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

  private void printConfiguredHosts() {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    
    LOG.debug("hostsReader: in=" + conf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, 
        YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH) + " out=" +
        conf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, 
            YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH));

    Set<String> hostsList = new HashSet<String>();
    Set<String> excludeList = new HashSet<String>();
    hostsReader.getHostDetails(hostsList, excludeList);

    for (String include : hostsList) {
      LOG.debug("include: " + include);
    }
    for (String exclude : excludeList) {
      LOG.debug("exclude: " + exclude);
    }
  }

  public void refreshNodes(Configuration yarnConf) throws IOException,
      YarnException {
    refreshHostsReader(yarnConf);

    for (NodeId nodeId: rmContext.getRMNodes().keySet()) {
      if (!isValidNode(nodeId.getHost())) {
        RMNodeEventType nodeEventType = isUntrackedNode(nodeId.getHost()) ?
            RMNodeEventType.SHUTDOWN : RMNodeEventType.DECOMMISSION;
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(nodeId, nodeEventType));
      }
    }
    updateInactiveNodes();
  }

  private void refreshHostsReader(Configuration yarnConf) throws IOException,
      YarnException {
    if (null == yarnConf) {
      yarnConf = new YarnConfiguration();
    }
    includesFile =
        yarnConf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
            YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH);
    excludesFile =
        yarnConf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
            YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH);
    hostsReader.refresh(includesFile, excludesFile);
    printConfiguredHosts();
  }

  private void setDecomissionedNMs() {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    for (final String host : excludeList) {
      NodeId nodeId = createUnknownNodeId(host);
      RMNodeImpl rmNode = new RMNodeImpl(nodeId,
          rmContext, host, -1, -1, new UnknownNode(host), null, null);
      rmContext.getInactiveRMNodes().put(nodeId, rmNode);
      rmNode.handle(new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
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
    String ip = resolver.resolve(hostName);
    Set<String> hostsList = new HashSet<String>();
    Set<String> excludeList = new HashSet<String>();
    hostsReader.getHostDetails(hostsList, excludeList);

    return (hostsList.isEmpty() || hostsList.contains(hostName) || hostsList
        .contains(ip))
        && !(excludeList.contains(hostName) || excludeList.contains(ip));
  }

  @Override
  public void handle(NodesListManagerEvent event) {
    RMNode eventNode = event.getNode();
    switch (event.getType()) {
    case NODE_UNUSABLE:
      LOG.debug(eventNode + " reported unusable");
      for(RMApp app: rmContext.getRMApps().values()) {
        if (!app.isAppFinalStateStored()) {
          this.rmContext
              .getDispatcher()
              .getEventHandler()
              .handle(
                  new RMAppNodeUpdateEvent(app.getApplicationId(), eventNode,
                      RMAppNodeUpdateType.NODE_UNUSABLE));
        }
      }
      break;
    case NODE_USABLE:
      LOG.debug(eventNode + " reported usable");
      for (RMApp app : rmContext.getRMApps().values()) {
        if (!app.isAppFinalStateStored()) {
          this.rmContext
              .getDispatcher()
              .getEventHandler()
              .handle(
                  new RMAppNodeUpdateEvent(app.getApplicationId(), eventNode,
                      RMAppNodeUpdateType.NODE_USABLE));
        }
      }
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
      setDecomissionedNMs();
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

    Set<String> hostsList = new HashSet<String>();
    Set<String> excludeList = new HashSet<String>();
    hostsReader.getHostDetails(hostsList, excludeList);

    return !hostsList.isEmpty() && !hostsList.contains(hostName)
        && !hostsList.contains(ip) && !excludeList.contains(hostName)
        && !excludeList.contains(ip);
  }

  /**
   * Refresh the nodes gracefully
   *
   * @param conf
   * @throws IOException
   * @throws YarnException
   */
  public void refreshNodesGracefully(Configuration conf) throws IOException,
      YarnException {
    refreshHostsReader(conf);
    for (Entry<NodeId, RMNode> entry : rmContext.getRMNodes().entrySet()) {
      NodeId nodeId = entry.getKey();
      if (!isValidNode(nodeId.getHost())) {
        RMNodeEventType nodeEventType = isUntrackedNode(nodeId.getHost()) ?
            RMNodeEventType.SHUTDOWN : RMNodeEventType.GRACEFUL_DECOMMISSION;
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(nodeId, nodeEventType));
      } else {
        // Recommissioning the nodes
        if (entry.getValue().getState() == NodeState.DECOMMISSIONING) {
          this.rmContext.getDispatcher().getEventHandler()
              .handle(new RMNodeEvent(nodeId, RMNodeEventType.RECOMMISSION));
        }
      }
    }
    updateInactiveNodes();
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

  /**
   * A NodeId instance needed upon startup for populating inactive nodes Map.
   * It only knows the hostname/ip and marks the port to -1 or invalid.
   */
  public static NodeId createUnknownNodeId(String host) {
    return NodeId.newInstance(host, -1);
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