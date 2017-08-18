/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ratis;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.ratis.client.ClientFactory;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.RpcType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Ratis helper methods.
 */
public interface RatisHelper {
  Logger LOG = LoggerFactory.getLogger(RatisHelper.class);

  static String toRaftPeerIdString(DatanodeID id) {
    return id.getIpAddr() + ":" + id.getRatisPort();
  }

  static RaftPeerId toRaftPeerId(DatanodeID id) {
    return RaftPeerId.valueOf(toRaftPeerIdString(id));
  }

  static RaftPeer toRaftPeer(String id) {
    return new RaftPeer(RaftPeerId.valueOf(id), id);
  }

  static RaftPeer toRaftPeer(DatanodeID id) {
    return toRaftPeer(toRaftPeerIdString(id));
  }

  static List<RaftPeer> toRaftPeers(Pipeline pipeline) {
    return pipeline.getMachines().stream()
        .map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
  }

  static RaftPeer[] toRaftPeerArray(Pipeline pipeline) {
    return toRaftPeers(pipeline).toArray(RaftPeer.EMPTY_PEERS);
  }

  static RaftClient newRaftClient(RpcType rpcType, Pipeline pipeline) {
    return newRaftClient(rpcType, toRaftPeerId(pipeline.getLeader()),
        toRaftPeers(pipeline));
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader) {
    return newRaftClient(rpcType, leader.getId(),
        new ArrayList<>(Arrays.asList(leader)));
  }

  static RaftClient newRaftClient(
      RpcType rpcType, RaftPeerId leader, List<RaftPeer> peers) {
    LOG.trace("newRaftClient: {}, leader={}, peers={}", rpcType, leader, peers);
    final RaftProperties properties = new RaftProperties();
    final ClientFactory factory = ClientFactory.cast(rpcType.newFactory(
        properties, null));

    return RaftClient.newBuilder()
        .setClientRpc(factory.newRaftClientRpc())
        .setServers(peers)
        .setLeaderId(leader)
        .setProperties(properties)
        .build();
  }
}
