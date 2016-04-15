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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.yarn.api.records.NodeId;
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

@SuppressWarnings("unchecked")
public class NodesListManager extends AbstractService implements
    EventHandler<NodesListManagerEvent> {

  private static final Log LOG = LogFactory.getLog(NodesListManager.class);

  private HostsFileReader hostsReader;
  private Configuration conf;
  private Set<RMNode> unusableRMNodesConcurrentSet = Collections
      .newSetFromMap(new ConcurrentHashMap<RMNode,Boolean>());
  
  private final RMContext rmContext;

  private String includesFile;
  private String excludesFile;

  public NodesListManager(RMContext rmContext) {
    super(NodesListManager.class.getName());
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.conf = conf;

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

    for (NodeId nodeId: rmContext.getRMNodes().keySet()) {
      if (!isValidNode(nodeId.getHost())) {
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
      }
    }
  }

  private void setDecomissionedNMs() {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    for (final String host : excludeList) {
      NodeId nodeId = createUnknownNodeId(host);
      RMNodeImpl rmNode = new RMNodeImpl(nodeId,
          rmContext, host, -1, -1, new UnknownNode(host), null, null);
      rmContext.getInactiveRMNodes().put(nodeId.getHost(), rmNode);
      rmNode.handle(new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
    }
  }

  public boolean isValidNode(String hostName) {
    synchronized (hostsReader) {
      Set<String> hostsList = hostsReader.getHosts();
      Set<String> excludeList = hostsReader.getExcludedHosts();
      String ip = NetUtils.normalizeHostName(hostName);
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

  /**
   * A NodeId instance needed upon startup for populating inactive nodes Map.
   * It only knows the hostname/ip and marks the port to -1 or invalid.
   */
  public static NodeId createUnknownNodeId(String host) {
    return NodeId.newInstance(host, -1);
  }

  /**
   * A Node instance needed upon startup for populating RMNode Map.
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
