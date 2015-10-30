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

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;

/**
 * Interface which is responsible for providing
 * the node {@literal ->} labels map.
 */
public abstract class RMNodeLabelsMappingProvider extends AbstractService {

  public RMNodeLabelsMappingProvider(String name) {
    super(name);
  }

  /**
   * Provides the labels. It is expected to give same Labels
   * continuously until there is a change in labels.
   *
   * @param nodes to fetch labels
   * @return Set of node label strings applicable for a node
   */
  public abstract Map<NodeId, Set<NodeLabel>> getNodeLabels(Set<NodeId> nodes);
}