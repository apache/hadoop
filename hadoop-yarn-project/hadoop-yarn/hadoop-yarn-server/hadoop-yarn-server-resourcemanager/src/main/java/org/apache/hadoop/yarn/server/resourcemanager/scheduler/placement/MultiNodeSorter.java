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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import com.google.common.annotations.VisibleForTesting;

/**
 * Common node sorting class which will do sorting based on policy spec.
 * @param <N> extends SchedulerNode.
 */
public class MultiNodeSorter<N extends SchedulerNode> extends AbstractService {

  private MultiNodeLookupPolicy<N> multiNodePolicy;
  private static final Log LOG = LogFactory.getLog(MultiNodeSorter.class);

  // ScheduledExecutorService which schedules the PreemptionChecker to run
  // periodically.
  private ScheduledExecutorService ses;
  private ScheduledFuture<?> handler;
  private volatile boolean stopped;
  private RMContext rmContext;
  private MultiNodePolicySpec policySpec;

  public MultiNodeSorter(RMContext rmContext,
      MultiNodePolicySpec policy) {
    super("MultiNodeLookupPolicy");
    this.rmContext = rmContext;
    this.policySpec = policy;
  }

  @VisibleForTesting
  public synchronized MultiNodeLookupPolicy<N> getMultiNodeLookupPolicy() {
    return multiNodePolicy;
  }

  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing MultiNodeSorter=" + policySpec.getPolicyName()
        + ", with sorting interval=" + policySpec.getSortingInterval());
    initPolicy(policySpec.getPolicyName());
    super.serviceInit(conf);
  }

  @SuppressWarnings("unchecked")
  void initPolicy(String policyName) throws YarnException {
    Class<?> policyClass;
    try {
      policyClass = Class.forName(policyName);
    } catch (ClassNotFoundException e) {
      throw new YarnException(
          "Invalid policy name:" + policyName + e.getMessage());
    }
    this.multiNodePolicy = (MultiNodeLookupPolicy<N>) ReflectionUtils
        .newInstance(policyClass, null);
  }

  @Override
  public void serviceStart() throws Exception {
    LOG.info("Starting SchedulingMonitor=" + getName());
    assert !stopped : "starting when already stopped";
    ses = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(getName());
        return t;
      }
    });

    // Start sorter thread only if sorting interval is a +ve value.
    if(policySpec.getSortingInterval() != 0) {
      handler = ses.scheduleAtFixedRate(new SortingThread(),
          0, policySpec.getSortingInterval(), TimeUnit.MILLISECONDS);
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    stopped = true;
    if (handler != null) {
      LOG.info("Stop " + getName());
      handler.cancel(true);
      ses.shutdown();
    }
    super.serviceStop();
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  public void reSortClusterNodes() {
    Set<String> nodeLabels = new HashSet<>();
    nodeLabels
        .addAll(rmContext.getNodeLabelManager().getClusterNodeLabelNames());
    nodeLabels.add(RMNodeLabelsManager.NO_LABEL);
    for (String label : nodeLabels) {
      Map<NodeId, SchedulerNode> nodesByPartition = new HashMap<>();
      List<SchedulerNode> nodes = ((AbstractYarnScheduler) rmContext
          .getScheduler()).getNodeTracker().getNodesPerPartition(label);
      if (nodes != null && !nodes.isEmpty()) {
        nodes.forEach(n -> nodesByPartition.put(n.getNodeID(), n));
        multiNodePolicy.addAndRefreshNodesSet(
            (Collection<N>) nodesByPartition.values(), label);
      }
    }
  }

  private class SortingThread implements Runnable {
    @Override
    public void run() {
      try {
        reSortClusterNodes();
      } catch (Throwable t) {
        // The preemption monitor does not alter structures nor do structures
        // persist across invocations. Therefore, log, skip, and retry.
        LOG.error("Exception raised while executing multinode"
            + " sorter, skip this run..., exception=", t);
      }
    }
  }

  /**
   * Verify whether sorter thread is running or not.
   *
   * @return true if sorter thread is running, false otherwise.
   */
  public boolean isSorterThreadRunning() {
    return (handler != null);
  }
}
