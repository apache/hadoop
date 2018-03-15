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

package org.apache.hadoop.ozone.scm.container.placement.algorithms;

import org.apache.hadoop.hdfs.protocol.DatanodeID;

import java.io.IOException;
import java.util.List;

/**
 * A ContainerPlacementPolicy support choosing datanodes to build replication
 * pipeline with specified constraints.
 */
public interface ContainerPlacementPolicy {

  /**
   * Given the replication factor and size required, return set of datanodes
   * that satisfy the nodes and size requirement.
   * @param nodesRequired - number of datanodes required.
   * @param sizeRequired - size required for the container or block.
   * @return list of datanodes chosen.
   * @throws IOException
   */
  List<DatanodeID> chooseDatanodes(int nodesRequired, long sizeRequired)
      throws IOException;
}
