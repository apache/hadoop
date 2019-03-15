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

package org.apache.hadoop.yarn.server.resourcemanager.blacklist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Maintains a list of failed nodes and returns that as long as number of
 * blacklisted nodes is below a threshold percentage of total nodes. If more
 * than threshold number of nodes are marked as failure they all are returned
 * as removal from blacklist so previous additions are reversed.
 */
public class SimpleBlacklistManager implements BlacklistManager {

  private int numberOfNodeManagerHosts;
  private final double blacklistDisableFailureThreshold;
  private final Set<String> blacklistNodes = new HashSet<>();
  private static final ArrayList<String> EMPTY_LIST = new ArrayList<>();

  private static final Logger LOG =
      LoggerFactory.getLogger(SimpleBlacklistManager.class);

  public SimpleBlacklistManager(int numberOfNodeManagerHosts,
      double blacklistDisableFailureThreshold) {
    this.numberOfNodeManagerHosts = numberOfNodeManagerHosts;
    this.blacklistDisableFailureThreshold = blacklistDisableFailureThreshold;
  }

  @Override
  public void addNode(String node) {
    blacklistNodes.add(node);
  }

  @Override
  public void refreshNodeHostCount(int nodeHostCount) {
    this.numberOfNodeManagerHosts = nodeHostCount;
  }

  @Override
  public ResourceBlacklistRequest getBlacklistUpdates() {
    ResourceBlacklistRequest ret;
    List<String> blacklist = new ArrayList<>(blacklistNodes);
    final int currentBlacklistSize = blacklist.size();
    final double failureThreshold = this.blacklistDisableFailureThreshold *
        numberOfNodeManagerHosts;
    if (currentBlacklistSize < failureThreshold) {
      LOG.debug("blacklist size {} is less than failure threshold ratio {}"
          + " out of total usable nodes {}", currentBlacklistSize,
          blacklistDisableFailureThreshold, numberOfNodeManagerHosts);
      ret = ResourceBlacklistRequest.newInstance(blacklist, EMPTY_LIST);
    } else {
      LOG.warn("Ignoring Blacklists, blacklist size " + currentBlacklistSize
          + " is more than failure threshold ratio "
          + blacklistDisableFailureThreshold + " out of total usable nodes "
          + numberOfNodeManagerHosts);
      // TODO: After the threshold hits, we will keep sending a long list
      // every time a new AM is to be scheduled.
      ret = ResourceBlacklistRequest.newInstance(EMPTY_LIST, blacklist);
    }
    return ret;
  }
}
