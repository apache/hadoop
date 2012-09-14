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

package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

public class AMNodeMap extends AbstractService implements
    EventHandler<AMNodeEvent> {
  
  static final Log LOG = LogFactory.getLog(AMNodeMap.class);
  
  private final ConcurrentHashMap<NodeId, AMNode> nodeMap;
  private final ConcurrentHashMap<String, ArrayList<NodeId>> blacklistMap;
  private final EventHandler<?> eventHandler;
  private final AppContext appContext;
  private int numClusterNodes;
  private boolean ignoreBlacklisting = false;
  private int maxTaskFailuresPerNode;
  private boolean nodeBlacklistingEnabled;
  private int blacklistDisablePercent;
  
  
  // TODO XXX Ensure there's a test for IgnoreBlacklisting in
  // TestRMContainerAllocator. Otherwise add one.
  public AMNodeMap(EventHandler<?> eventHandler, AppContext appContext) {
    super("AMNodeMap");
    this.nodeMap = new ConcurrentHashMap<NodeId, AMNode>();
    this.blacklistMap = new ConcurrentHashMap<String, ArrayList<NodeId>>();
    this.eventHandler = eventHandler;
    this.appContext = appContext;
  }
  
  @Override
  public synchronized void init(Configuration config) {
    Configuration conf = new Configuration(config);
    this.maxTaskFailuresPerNode = conf.getInt(
        MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 3);
    this.nodeBlacklistingEnabled = config.getBoolean(
        MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    this.blacklistDisablePercent = config.getInt(
          MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT,
          MRJobConfig.DEFAULT_MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERCENT);

    LOG.info("blacklistDisablePercent is " + blacklistDisablePercent +
        ", blacklistingEnabled: " + nodeBlacklistingEnabled + 
        ", maxTaskFailuresPerNode: " + maxTaskFailuresPerNode);

    if (blacklistDisablePercent < -1 || blacklistDisablePercent > 100) {
      throw new YarnException("Invalid blacklistDisablePercent: "
          + blacklistDisablePercent
          + ". Should be an integer between 0 and 100 or -1 to disabled");
    }    
    super.init(conf);
  }
  
  
  public void nodeSeen(NodeId nodeId) {
    nodeMap.putIfAbsent(nodeId, new AMNodeImpl(
        nodeId, maxTaskFailuresPerNode, eventHandler, appContext));
  }
  
  public boolean isHostBlackListed(String hostname) {
    // TODO Maybe: For now, forced unblacklisting is being handled
    // here. An AMNode will never go into the BLACKLISTED / FORCED_ACTIVE state.
    if (!nodeBlacklistingEnabled || ignoreBlacklisting) {
      return false;
    }
    return blacklistMap.containsKey(hostname);
  }

  private void addToBlackList(NodeId nodeId) {
    String host = nodeId.getHost();
    ArrayList<NodeId> nodes;
    
    if (!blacklistMap.containsKey(host)) {
      nodes = new ArrayList<NodeId>();
      blacklistMap.put(host, nodes);
    } else {
      nodes = blacklistMap.get(host);
    }
    
    if (!nodes.contains(nodeId)) {
      nodes.add(nodeId);
    }
  }
  
  // TODO: Currently, un-blacklisting feature is not supported.
  /*
  private void removeFromBlackList(NodeId nodeId) {
    String host = nodeId.getHost();
    if (blacklistMap.containsKey(host)) {
      ArrayList<NodeId> nodes = blacklistMap.get(host);
      nodes.remove(nodeId);
    }
  }
  */

  public void handle(AMNodeEvent rEvent) {
    // No synchronization required until there's multiple dispatchers.
    NodeId nodeId = rEvent.getNodeId();
    switch (rEvent.getType()) {
    case N_NODE_WAS_BLACKLISTED:
      addToBlackList(nodeId);
      computeIgnoreBlacklisting();
      break;
    case N_NODE_COUNT_UPDATED:
      AMNodeEventNodeCountUpdated event = (AMNodeEventNodeCountUpdated) rEvent;
      numClusterNodes = event.getNodeCount();
      computeIgnoreBlacklisting();
      break;
    default:
      nodeMap.get(nodeId).handle(rEvent);
    }
  }

  // May be incorrect if there's multiple NodeManagers running on a single host.
  // knownNodeCount is based on node managers, not hosts. blacklisting is
  // currently based on hosts.
  protected void computeIgnoreBlacklisting() {
    if (!nodeBlacklistingEnabled) {
      return;
    }
    if (blacklistDisablePercent != -1) {
      if (numClusterNodes == 0) {
        LOG.info("KnownNode Count at 0. Not computing ignoreBlacklisting");
        return;
      }
      int val = (int) ((float) blacklistMap.size() / numClusterNodes * 100);
      if (val >= blacklistDisablePercent) {
        if (ignoreBlacklisting == false) {
          ignoreBlacklisting = true;
          LOG.info("Ignore Blacklisting set to true. Known: " + numClusterNodes
              + ", Blacklisted: " + blacklistMap.size());
        }
      } else {
        if (ignoreBlacklisting == true) {
          ignoreBlacklisting = false;
          LOG.info("Ignore blacklisting set to false. Known: "
              + numClusterNodes + ", Blacklisted: " + blacklistMap.size());
        }
      }
    }
  }

  public AMNode get(NodeId nodeId) {
    return nodeMap.get(nodeId);
  }

  public int size() {
    return nodeMap.size();
  }
  
  @Private
  public boolean isBlacklistingIgnored() {
    return this.ignoreBlacklisting;
  }
}