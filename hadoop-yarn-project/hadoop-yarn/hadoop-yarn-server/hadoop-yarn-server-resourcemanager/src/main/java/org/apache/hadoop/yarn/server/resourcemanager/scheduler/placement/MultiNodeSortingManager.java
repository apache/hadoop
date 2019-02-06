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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

/**
 * Node Sorting Manager which runs all sorter threads and policies.
 * @param <N> extends SchedulerNode
 */
public class MultiNodeSortingManager<N extends SchedulerNode>
    extends AbstractService {

  private static final Log LOG = LogFactory
      .getLog(MultiNodeSortingManager.class);

  private RMContext rmContext;
  private Map<String, MultiNodeSorter<N>> runningMultiNodeSorters;
  private Set<MultiNodePolicySpec> policySpecs = new HashSet<MultiNodePolicySpec>();
  private Configuration conf;
  private boolean multiNodePlacementEnabled;

  public MultiNodeSortingManager() {
    super("MultiNodeSortingManager");
    this.runningMultiNodeSorters = new ConcurrentHashMap<>();
  }

  @Override
  public void serviceInit(Configuration configuration) throws Exception {
    LOG.info("Initializing NodeSortingService=" + getName());
    super.serviceInit(configuration);
    this.conf = configuration;
  }

  @Override
  public void serviceStart() throws Exception {
    LOG.info("Starting NodeSortingService=" + getName());
    createAllPolicies();
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    for (MultiNodeSorter<N> sorter : runningMultiNodeSorters.values()) {
      sorter.stop();
    }
    super.serviceStop();
  }

  private void createAllPolicies() {
    if (!multiNodePlacementEnabled) {
      return;
    }
    for (MultiNodePolicySpec policy : policySpecs) {
      MultiNodeSorter<N> mon = new MultiNodeSorter<N>(rmContext, policy);
      mon.init(conf);
      mon.start();
      runningMultiNodeSorters.put(policy.getPolicyName(), mon);
    }
  }

  public MultiNodeSorter<N> getMultiNodePolicy(String name) {
    return runningMultiNodeSorters.get(name);
  }

  public void setRMContext(RMContext context) {
    this.rmContext = context;
  }

  public void registerMultiNodePolicyNames(
      boolean isMultiNodePlacementEnabled,
      Set<MultiNodePolicySpec> multiNodePlacementPolicies) {
    this.policySpecs.addAll(multiNodePlacementPolicies);
    this.multiNodePlacementEnabled = isMultiNodePlacementEnabled;
    LOG.info("MultiNode scheduling is '" + multiNodePlacementEnabled +
        "', and configured policies are " + StringUtils
        .join(policySpecs.iterator(), ","));
  }

  public Iterator<N> getMultiNodeSortIterator(Collection<N> nodes,
      String partition, String policyName) {
    // nodeLookupPolicy can be null if app is configured with invalid policy.
    // in such cases, use the the first node.
    if(policyName == null) {
      LOG.warn("Multi Node scheduling is enabled, however invalid class is"
          + " configured. Valid sorting policy has to be configured in"
          + " yarn.scheduler.capacity.<queue>.multi-node-sorting.policy");
      return IteratorUtils.singletonIterator(
          nodes.iterator().next());
    }

    MultiNodeSorter multiNodeSorter = getMultiNodePolicy(policyName);
    if (multiNodeSorter == null) {
      LOG.warn(
          "MultiNode policy '" + policyName + "' is configured, however " +
              "yarn.scheduler.capacity.multi-node-placement-enabled is false");
      return IteratorUtils.singletonIterator(
          nodes.iterator().next());
    }

    MultiNodeLookupPolicy<N> policy = multiNodeSorter
        .getMultiNodeLookupPolicy();
    // If sorter thread is not running, refresh node set.
    if (!multiNodeSorter.isSorterThreadRunning()) {
      policy.addAndRefreshNodesSet(nodes, partition);
    }

    return policy.getPreferredNodeIterator(nodes, partition);
  }
}
