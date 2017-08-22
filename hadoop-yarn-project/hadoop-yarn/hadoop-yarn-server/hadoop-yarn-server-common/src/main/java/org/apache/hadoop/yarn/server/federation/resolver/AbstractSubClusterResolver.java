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

package org.apache.hadoop.yarn.server.federation.resolver;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Partial implementation of {@link SubClusterResolver}, containing basic
 * implementations of the read methods.
 */
public abstract class AbstractSubClusterResolver implements SubClusterResolver {
  private Map<String, SubClusterId> nodeToSubCluster =
      new ConcurrentHashMap<String, SubClusterId>();
  private Map<String, Set<SubClusterId>> rackToSubClusters =
      new ConcurrentHashMap<String, Set<SubClusterId>>();

  @Override
  public SubClusterId getSubClusterForNode(String nodename)
      throws YarnException {
    SubClusterId subClusterId = this.nodeToSubCluster.get(nodename);

    if (subClusterId == null) {
      throw new YarnException("Cannot find subClusterId for node " + nodename);
    }

    return subClusterId;
  }

  @Override
  public Set<SubClusterId> getSubClustersForRack(String rackname)
      throws YarnException {
    if (!rackToSubClusters.containsKey(rackname)) {
      throw new YarnException("Cannot resolve rack " + rackname);
    }

    return rackToSubClusters.get(rackname);
  }

  public Map<String, SubClusterId> getNodeToSubCluster() {
    return nodeToSubCluster;
  }

  public Map<String, Set<SubClusterId>> getRackToSubClusters() {
    return rackToSubClusters;
  }
}
