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

package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.NetworkTopology;

/**
 * Space balanced block placement policy.
 */
public class AvailableSpaceBlockPlacementPolicy extends
    BlockPlacementPolicyDefault {
  private static final Log LOG = LogFactory
      .getLog(AvailableSpaceBlockPlacementPolicy.class);
  private static final Random RAND = new Random();
  private int balancedPreference =
      (int) (100 * DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);

  @Override
  public void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    float balancedPreferencePercent =
        conf.getFloat(
          DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
          DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);

    LOG.info("Available space block placement policy initialized: "
        + DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
        + " = " + balancedPreferencePercent);

    if (balancedPreferencePercent > 1.0) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is greater than 1.0 but should be in the range 0.0 - 1.0");
    }
    if (balancedPreferencePercent < 0.5) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is less than 0.5 so datanodes with more used percent will"
          + " receive  more block allocations.");
    }
    balancedPreference = (int) (100 * balancedPreferencePercent);
  }

  @Override
  protected DatanodeDescriptor chooseDataNode(String scope) {
    DatanodeDescriptor a = (DatanodeDescriptor) clusterMap.chooseRandom(scope);
    DatanodeDescriptor b = (DatanodeDescriptor) clusterMap.chooseRandom(scope);
    if (a != null && b != null){
      int ret = compareDataNode(a, b);
      if (ret == 0) {
        return a;
      } else if (ret < 0) {
        return (RAND.nextInt(100) < balancedPreference) ? a : b;
      } else {
        return (RAND.nextInt(100) < balancedPreference) ? b : a;
      }
    } else {
      return a == null ? b : a;
    }
  }

  /**
   * Compare the two data nodes.
   */
  protected int compareDataNode(final DatanodeDescriptor a,
      final DatanodeDescriptor b) {
    if (a.equals(b)
        || Math.abs(a.getDfsUsedPercent() - b.getDfsUsedPercent()) < 5) {
      return 0;
    }
    return a.getDfsUsedPercent() < b.getDfsUsedPercent() ? -1 : 1;
  }
}
