/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;

import java.io.IOException;
import java.util.List;

/**
 * A PlacementPolicy support choosing datanodes to build
 * pipelines or containers with specified constraints.
 */
public interface PlacementPolicy {

  /**
   * Given an initial set of datanodes and the size required,
   * return set of datanodes that satisfy the nodes and size requirement.
   *
   * @param excludedNodes - list of nodes to be excluded.
   * @param favoredNodes - list of nodes preferred.
   * @param nodesRequired - number of datanodes required.
   * @param sizeRequired - size required for the container or block.
   * @return list of datanodes chosen.
   * @throws IOException
   */
  List<DatanodeDetails> chooseDatanodes(List<DatanodeDetails> excludedNodes,
      List<DatanodeDetails> favoredNodes, int nodesRequired, long sizeRequired)
      throws IOException;
}
