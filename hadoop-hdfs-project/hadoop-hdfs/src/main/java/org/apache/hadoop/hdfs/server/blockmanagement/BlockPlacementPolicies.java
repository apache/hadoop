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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.ReflectionUtils;

@InterfaceAudience.Private
public class BlockPlacementPolicies{

  private final BlockPlacementPolicy replicationPolicy;
  private final BlockPlacementPolicy ecPolicy;

  public BlockPlacementPolicies(Configuration conf, FSClusterStats stats,
                                NetworkTopology clusterMap,
                                Host2NodesMap host2datanodeMap){
    final Class<? extends BlockPlacementPolicy> replicatorClass = conf
        .getClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
            DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_DEFAULT,
            BlockPlacementPolicy.class);
    replicationPolicy = ReflectionUtils.newInstance(replicatorClass, conf);
    replicationPolicy.initialize(conf, stats, clusterMap, host2datanodeMap);
    final Class<? extends BlockPlacementPolicy> blockPlacementECClass =
        conf.getClass(DFSConfigKeys.DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY,
            DFSConfigKeys.DFS_BLOCK_PLACEMENT_EC_CLASSNAME_DEFAULT,
            BlockPlacementPolicy.class);
    ecPolicy = ReflectionUtils.newInstance(blockPlacementECClass, conf);
    ecPolicy.initialize(conf, stats, clusterMap, host2datanodeMap);
  }

  public BlockPlacementPolicy getPolicy(BlockType blockType){
    switch (blockType) {
    case CONTIGUOUS: return replicationPolicy;
    case STRIPED: return ecPolicy;
    default:
      throw new IllegalArgumentException(
          "getPolicy received a BlockType that isn't supported.");
    }
  }
}
