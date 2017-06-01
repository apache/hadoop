/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm.ratis;


import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;

import java.io.IOException;
import java.util.List;

/**
 * Manage Ratis clusters.
 */
public interface RatisManager {
  /**
   * Create a new Ratis cluster with the given clusterId and datanodes.
   */
  void createRatisCluster(String clusterId, List<DatanodeID> datanodes)
      throws IOException;

  /**
   * Close the Ratis cluster with the given clusterId.
   */
  void closeRatisCluster(String clusterId) throws IOException;

  /**
   * @return the datanode list of the Ratis cluster with the given clusterId.
   */
  List<DatanodeID> getDatanodes(String clusterId) throws IOException;

  /**
   * Update the datanode list of the Ratis cluster with the given clusterId.
   */
  void updateDatanodes(String clusterId, List<DatanodeID> newDatanodes)
      throws IOException;

  static RatisManager newRatisManager(OzoneConfiguration conf) {
    final String rpc = conf.get(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    return new RatisManagerImpl(rpc);
  }
}
