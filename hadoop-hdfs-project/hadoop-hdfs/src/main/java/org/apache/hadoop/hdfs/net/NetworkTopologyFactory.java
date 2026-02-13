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
package org.apache.hadoop.hdfs.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.NetworkTopology;

/**
 * Factory for creating a {@link NetworkTopology} instance based on
 * the {@code dfs.use.dfs.network.topology} configuration.
 */
public final class NetworkTopologyFactory {

  private NetworkTopologyFactory() {}

  /**
   * Creates a {@link NetworkTopology} instance.
   * If {@code dfs.use.dfs.network.topology} is enabled, returns a
   * {@link DFSNetworkTopology}; otherwise, returns a plain
   * {@link NetworkTopology}.
   */
  public static NetworkTopology create(Configuration conf) {
    boolean useDfsNetworkTopology = conf.getBoolean(
        DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY,
        DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_DEFAULT);
    if (useDfsNetworkTopology) {
      return DFSNetworkTopology.getInstance(conf);
    } else {
      return NetworkTopology.getInstance(conf);
    }
  }
}