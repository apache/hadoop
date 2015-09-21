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
import java.util.Collection;
import java.util.Collections;
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
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.HostsFileReader;
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

@SuppressWarnings("unchecked")
public class NodesListManager extends CompositeService implements
    EventHandler<NodesListManagerEvent> {

  private static final Log LOG = LogFactory.getLog(NodesListManager.class);

  private HostsFileReader hostsReader;
  private Configuration conf;
  private Set<RMNode> unusableRMNodesConcurrentSet = Collections
      .newSetFromMap(new ConcurrentHashMap<RMNode,Boolean>());
  
  private final RMContext rmContext;

  private String includesFile;
  private String excludesFile;

  private Resolver resolver;

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
      resolver = new CachedResolver(new SystemClock(), nodeIpCacheTimeout);
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
      setDecomissionedNMsMetrics();
      printConfiguredHosts();
    } catch (YarnException ex) {
      disableHostsFileReader(ex);
    } catch (IOException ioe) {
      disableHostsFileReader(ioe);
    }
    super.serviceInit(conf);
  }

  private void printConfiguredHosts() {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    
    LOG.debug("hostsReader: in=" + conf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH, 
        YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH) + " out=" +
        conf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, 
            YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH));
    for (String include : hostsReader.getHosts()) {
      LOG.debug("include: " + include);
    }
    for (String exclude : hostsReader.getExcludedHosts()) {
      LOG.debug("exclude: " + exclude);
    }
  }

  public void refreshNodes(Configuration yarnConf) throws IOException,
      YarnException {
    refreshHostsReader(yarnConf);

    for (NodeId nodeId: rmContext.getRMNodes().keySet()) {
      if (!isValidNode(nodeId.getHost())) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
      }
    }
  }

  private void refreshHostsReader(Configuration yarnConf) throws IOException,
      YarnException {
    synchronized (hostsReader) {
      if (null == yarnConf) {
        yarnConf = new YarnConfiguration();
      }
      includesFile =
          yarnConf.get(YarnConfiguration.RM_NODES_INCLUDE_FILE_PATH,
              YarnConfiguration.DEFAULT_RM_NODES_INCLUDE_FILE_PATH);
      excludesFile =
          yarnConf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
              YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH);
      hostsReader.updateFileNames(includesFile, excludesFile);
      hostsReader.refresh(
          includesFile.isEmpty() ? null : this.rmContext
              .getConfigurationProvider().getConfigurationInputStream(
                  this.conf, includesFile), excludesFile.isEmpty() ? null
              : this.rmContext.getConfigurationProvider()
                  .getConfigurationInputStream(this.conf, excludesFile));
      printConfiguredHosts();
    }
  }

  private void setDecomissionedNMsMetrics() {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    ClusterMetrics.getMetrics().setDecommisionedNMs(excludeList.size());
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
    synchronized (hostsReader) {
      Set<String> hostsList = hostsReader.getHosts();
      Set<String> excludeList = hostsReader.getExcludedHosts();
      return (hostsList.isEmpty() || hostsList.contains(hostName) || hostsList
          .contains(ip))
          && !(excludeList.contains(hostName) || excludeList.contains(ip));
    }
  }

  /**
   * Provides the currently unusable nodes. Copies it into provided collection.
   * @param unUsableNodes
   *          Collection to which the unusable nodes are added
   * @return number of unusable nodes added
   */
  public int getUnusableNodes(Collection<RMNode> unUsableNodes) {
    unUsableNodes.addAll(unusableRMNodesConcurrentSet);
    return unusableRMNodesConcurrentSet.size();
  }

  @Override
  public void handle(NodesListManagerEvent event) {
    RMNode eventNode = event.getNode();
    switch (event.getType()) {
    case NODE_UNUSABLE:
      LOG.debug(eventNode + " reported unusable");
      unusableRMNodesConcurrentSet.add(eventNode);
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
      if (unusableRMNodesConcurrentSet.contains(eventNode)) {
        LOG.debug(eventNode + " reported usable");
        unusableRMNodesConcurrentSet.remove(eventNode);
      }
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
      setDecomissionedNMsMetrics();
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
    for (Entry<NodeId, RMNode> entry:rmContext.getRMNodes().entrySet()) {
      NodeId nodeId = entry.getKey();
      if (!isValidNode(nodeId.getHost())) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(nodeId, RMNodeEventType.GRACEFUL_DECOMMISSION));
      } else {
        // Recommissioning the nodes
        if (entry.getValue().getState() == NodeState.DECOMMISSIONING
            || entry.getValue().getState() == NodeState.DECOMMISSIONED) {
          this.rmContext.getDispatcher().getEventHandler()
              .handle(new RMNodeEvent(nodeId, RMNodeEventType.RECOMMISSION));
        }
      }
    }
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
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(entry.getKey(), RMNodeEventType.DECOMMISSION));
      }
    }
  }
}