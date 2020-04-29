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
package org.apache.hadoop.tools.dynamometer;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatus;

/**
 * A BlockPlacementPolicy which always considered itself satisfied. This avoids
 * the issue that the Dynamometer NameNode will complain about blocks being
 * under-replicated because they're not being put on distinct racks.
 */
public class BlockPlacementPolicyAlwaysSatisfied
    extends BlockPlacementPolicyDefault {

  private static final BlockPlacementStatusSatisfied SATISFIED =
      new BlockPlacementStatusSatisfied();

  private static class BlockPlacementStatusSatisfied
      implements BlockPlacementStatus {
    @Override
    public boolean isPlacementPolicySatisfied() {
      return true;
    }

    public String getErrorDescription() {
      return null;
    }

    @Override
    public int getAdditionalReplicasRequired() {
      return 0;
    }
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    return SATISFIED;
  }

}
