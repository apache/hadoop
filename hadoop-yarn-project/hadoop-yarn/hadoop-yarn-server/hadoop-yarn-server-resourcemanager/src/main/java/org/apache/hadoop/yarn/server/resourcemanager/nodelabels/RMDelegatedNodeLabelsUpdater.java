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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Update nodes labels map for ResourceManager periodically. It collects
 * nodes labels from {@link RMNodeLabelsMappingProvider} and updates the
 * nodes {@literal ->} labels map via {@link RMNodeLabelsManager}.
 * This service is enabled when configuration
 * "yarn.node-labels.configuration-type" is set to "delegated-centralized".
 */
public class RMDelegatedNodeLabelsUpdater extends CompositeService {

  private static final Logger LOG = LoggerFactory
      .getLogger(RMDelegatedNodeLabelsUpdater.class);

  public static final long DISABLE_DELEGATED_NODE_LABELS_UPDATE = -1;

  // Timer used to schedule node labels fetching
  private Timer nodeLabelsScheduler;
  // 30 seconds
  @VisibleForTesting
  public long nodeLabelsUpdateInterval = 30 * 1000;

  private Set<NodeId> newlyRegisteredNodes = new HashSet<NodeId>();
  // Lock to protect newlyRegisteredNodes
  private Object lock = new Object();
  private long lastAllNodesLabelUpdateMills = 0L;
  private long allNodesLabelUpdateInterval;

  private RMNodeLabelsMappingProvider rmNodeLabelsMappingProvider;

  private RMContext rmContext;

  public RMDelegatedNodeLabelsUpdater(RMContext rmContext) {
    super("RMDelegatedNodeLabelsUpdater");
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    allNodesLabelUpdateInterval = conf.getLong(
        YarnConfiguration.RM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS);
    rmNodeLabelsMappingProvider = createRMNodeLabelsMappingProvider(conf);
    addService(rmNodeLabelsMappingProvider);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    nodeLabelsScheduler = new Timer(
        "RMDelegatedNodeLabelsUpdater-Timer", true);
    TimerTask delegatedNodeLabelsUpdaterTimerTask =
        new RMDelegatedNodeLabelsUpdaterTimerTask();
    nodeLabelsScheduler.scheduleAtFixedRate(
        delegatedNodeLabelsUpdaterTimerTask,
        nodeLabelsUpdateInterval,
        nodeLabelsUpdateInterval);

    super.serviceStart();
  }

  /**
   * Terminate the timer.
   * @throws Exception
   */
  @Override
  protected void serviceStop() throws Exception {
    if (nodeLabelsScheduler != null) {
      nodeLabelsScheduler.cancel();
    }
    super.serviceStop();
  }

  private class RMDelegatedNodeLabelsUpdaterTimerTask extends TimerTask {
    @Override
    public void run() {
      Set<NodeId> nodesToUpdateLabels = null;
      boolean isUpdatingAllNodes = false;

      if (allNodesLabelUpdateInterval != DISABLE_DELEGATED_NODE_LABELS_UPDATE) {
        long elapsedTimeSinceLastUpdate =
            System.currentTimeMillis() - lastAllNodesLabelUpdateMills;
        if (elapsedTimeSinceLastUpdate > allNodesLabelUpdateInterval) {
          nodesToUpdateLabels =
              Collections.unmodifiableSet(rmContext.getRMNodes().keySet());
          isUpdatingAllNodes = true;
        }
      }

      if (nodesToUpdateLabels == null && !newlyRegisteredNodes.isEmpty()) {
        synchronized (lock) {
          if (!newlyRegisteredNodes.isEmpty()) {
            nodesToUpdateLabels = new HashSet<NodeId>(newlyRegisteredNodes);
          }
        }
      }

      try {
        if (nodesToUpdateLabels != null && !nodesToUpdateLabels.isEmpty()) {
          updateNodeLabelsInternal(nodesToUpdateLabels);
          if (isUpdatingAllNodes) {
            lastAllNodesLabelUpdateMills = System.currentTimeMillis();
          }
          synchronized (lock) {
            newlyRegisteredNodes.removeAll(nodesToUpdateLabels);
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to update node Labels", e);
      }
    }
  }

  private void updateNodeLabelsInternal(Set<NodeId> nodes)
      throws IOException {
    Map<NodeId, Set<NodeLabel>> labelsUpdated =
        rmNodeLabelsMappingProvider.getNodeLabels(nodes);
    if (labelsUpdated != null && labelsUpdated.size() != 0) {
      Map<NodeId, Set<String>> nodeToLabels =
          new HashMap<NodeId, Set<String>>(labelsUpdated.size());
      for (Map.Entry<NodeId, Set<NodeLabel>> entry
          : labelsUpdated.entrySet()) {
        nodeToLabels.put(entry.getKey(),
            NodeLabelsUtils.convertToStringSet(entry.getValue()));
      }
      rmContext.getNodeLabelManager().replaceLabelsOnNode(nodeToLabels);
    }
  }

  /**
   * Get the RMNodeLabelsMappingProvider which is used to provide node labels.
   */
  private RMNodeLabelsMappingProvider createRMNodeLabelsMappingProvider(
      Configuration conf) throws IOException {
    RMNodeLabelsMappingProvider nodeLabelsMappingProvider = null;
    try {
      Class<? extends RMNodeLabelsMappingProvider> labelsProviderClass =
          conf.getClass(YarnConfiguration.RM_NODE_LABELS_PROVIDER_CONFIG,
              null, RMNodeLabelsMappingProvider.class);
      if (labelsProviderClass != null) {
        nodeLabelsMappingProvider = labelsProviderClass.newInstance();
      }
    } catch (InstantiationException | IllegalAccessException
        | RuntimeException e) {
      LOG.error("Failed to create RMNodeLabelsMappingProvider based on"
          + " Configuration", e);
      throw new IOException("Failed to create RMNodeLabelsMappingProvider : "
          + e.getMessage(), e);
    }

    if (nodeLabelsMappingProvider == null) {
      String msg = "RMNodeLabelsMappingProvider should be configured when "
          + "delegated-centralized node label configuration is enabled";
      LOG.error(msg);
      throw new IOException(msg);
    } else {
      LOG.debug("RM Node labels mapping provider class is : {}",
          nodeLabelsMappingProvider.getClass());
    }

    return nodeLabelsMappingProvider;
  }

  /**
   * Update node labels for a specified node.
   * @param node the node to update node labels
   */
  public void updateNodeLabels(NodeId node) {
    synchronized (lock) {
      newlyRegisteredNodes.add(node);
    }
  }
}
