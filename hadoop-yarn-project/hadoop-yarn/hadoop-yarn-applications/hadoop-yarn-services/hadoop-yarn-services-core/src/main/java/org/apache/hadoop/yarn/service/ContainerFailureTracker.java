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

package org.apache.hadoop.yarn.service;

import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.DEFAULT_NODE_BLACKLIST_THRESHOLD;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.NODE_BLACKLIST_THRESHOLD;

/**
 * This tracks the container failures per node. If the failure counter exceeds
 * the maxFailurePerNode limit, it'll blacklist that node.
 *
 */
public class ContainerFailureTracker {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerFailureTracker.class);

  // Host -> num container failures
  private Map<String, Integer> failureCountPerNode = new HashMap<>();
  private Set<String> blackListedNodes = new HashSet<>();
  private ServiceContext context;
  private int maxFailurePerNode;
  private Component component;

  public ContainerFailureTracker(ServiceContext context, Component component) {
    this.context = context;
    this.component = component;
    maxFailurePerNode = YarnServiceConf.getInt(NODE_BLACKLIST_THRESHOLD,
        DEFAULT_NODE_BLACKLIST_THRESHOLD, component.getComponentSpec()
        .getConfiguration(), context.scheduler.getConfig());
  }


  public synchronized void incNodeFailure(String host) {
    int num = 0;
    if (failureCountPerNode.containsKey(host)) {
      num = failureCountPerNode.get(host);
    }
    num++;
    failureCountPerNode.put(host, num);

    // black list the node if exceed max failure
    if (num > maxFailurePerNode && !blackListedNodes.contains(host)) {
      List<String> blacklists = new ArrayList<>();
      blacklists.add(host);
      blackListedNodes.add(host);
      context.scheduler.getAmRMClient().updateBlacklist(blacklists, null);
      LOG.info("[COMPONENT {}]: Failed {} times on this host, blacklisted {}."
              + " Current list of blacklisted nodes: {}",
          component.getName(), num, host, blackListedNodes);
    }
  }

  public synchronized void resetContainerFailures() {
    // reset container failure counter per node
    failureCountPerNode.clear();
    context.scheduler.getAmRMClient()
        .updateBlacklist(null, new ArrayList<>(blackListedNodes));
    LOG.info("[COMPONENT {}]: Clearing blacklisted nodes {} ",
        component.getName(), blackListedNodes);
    blackListedNodes.clear();
  }

}
