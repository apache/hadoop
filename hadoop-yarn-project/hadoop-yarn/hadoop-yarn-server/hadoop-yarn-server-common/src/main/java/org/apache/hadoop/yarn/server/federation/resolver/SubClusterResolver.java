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

import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * An utility that helps to determine the sub-cluster that a specified node or
 * rack belongs to. All implementing classes should be thread-safe.
 */
public interface SubClusterResolver extends Configurable {

  /**
   * Obtain the sub-cluster that a specified node belongs to.
   *
   * @param nodename the node whose sub-cluster is to be determined
   * @return the sub-cluster as identified by the {@link SubClusterId} that the
   *         node belongs to
   * @throws YarnException if the node's sub-cluster cannot be resolved
   */
  SubClusterId getSubClusterForNode(String nodename) throws YarnException;

  /**
   * Obtain the sub-clusters that have nodes on a specified rack.
   *
   * @param rackname the name of the rack
   * @return the sub-clusters as identified by the {@link SubClusterId} that
   *         have nodes on the given rack
   * @throws YarnException if the sub-cluster of any node on the rack cannot be
   *           resolved, or if the rack name is not recognized
   */
  Set<SubClusterId> getSubClustersForRack(String rackname) throws YarnException;

  /**
   * Load the nodes to subCluster mapping from the file.
   */
  void load();
}
