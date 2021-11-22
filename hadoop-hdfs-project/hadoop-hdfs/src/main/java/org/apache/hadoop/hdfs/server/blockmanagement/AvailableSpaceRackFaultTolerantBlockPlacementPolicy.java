/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Random;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_KEY;

/**
 * Space balanced rack fault tolerant block placement policy.
 */
public class AvailableSpaceRackFaultTolerantBlockPlacementPolicy
    extends BlockPlacementPolicyRackFaultTolerant {

  private static final Logger LOG = LoggerFactory
      .getLogger(AvailableSpaceRackFaultTolerantBlockPlacementPolicy.class);
  private static final Random RAND = new Random();
  private int balancedPreference = (int) (100
      * DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);
  private int balancedSpaceTolerance =
        DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT;
  @Override
  public void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    float balancedPreferencePercent = conf.getFloat(
        DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
        DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);

    balancedSpaceTolerance = conf.getInt(
            DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_KEY,
            DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT);

    LOG.info("Available space rack fault tolerant block placement policy "
        + "initialized: "
        + DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
        + " = " + balancedPreferencePercent);

    if (balancedPreferencePercent > 1.0) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is greater than 1.0 but should be in the range 0.0 - 1.0");
    }
    if (balancedPreferencePercent < 0.5) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is less than 0.5 so datanodes with more used percent will"
          + " receive  more block allocations.");
    }


    if (balancedSpaceTolerance > 20 || balancedSpaceTolerance < 0) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_KEY
          + " is invalid, Current value is " + balancedSpaceTolerance + ", Default value " +
            DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT
          + " will be used instead.");
      balancedSpaceTolerance =
            DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT;
    }

    balancedPreference = (int) (100 * balancedPreferencePercent);
  }

  @Override
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNode, StorageType type) {
    // only the code that uses DFSNetworkTopology should trigger this code path.
    Preconditions.checkArgument(clusterMap instanceof DFSNetworkTopology);
    DFSNetworkTopology dfsClusterMap = (DFSNetworkTopology) clusterMap;
    DatanodeDescriptor a = (DatanodeDescriptor) dfsClusterMap
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNode, type);
    DatanodeDescriptor b = (DatanodeDescriptor) dfsClusterMap
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNode, type);
    return select(a, b);
  }

  @Override
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNode) {
    DatanodeDescriptor a =
        (DatanodeDescriptor) clusterMap.chooseRandom(scope, excludedNode);
    DatanodeDescriptor b =
        (DatanodeDescriptor) clusterMap.chooseRandom(scope, excludedNode);
    return select(a, b);
  }

  private DatanodeDescriptor select(DatanodeDescriptor a,
      DatanodeDescriptor b) {
    if (a != null && b != null) {
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
        || Math.abs(a.getDfsUsedPercent() - b.getDfsUsedPercent()) < balancedSpaceTolerance) {
      return 0;
    }
    return a.getDfsUsedPercent() < b.getDfsUsedPercent() ? -1 : 1;
  }
}