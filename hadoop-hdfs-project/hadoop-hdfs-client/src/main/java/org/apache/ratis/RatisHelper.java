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
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Collections;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Ratis helper methods.
 */
public interface RatisHelper {
  Logger LOG = LoggerFactory.getLogger(RatisHelper.class);

  static String toRaftPeerIdString(DatanodeID id) {
    return id.getIpAddr() + "_" + id.getRatisPort();
  }

  static String toRaftPeerAddressString(DatanodeID id) {
    return id.getIpAddr() + ":" + id.getRatisPort();
  }

  static RaftPeerId toRaftPeerId(DatanodeID id) {
    return RaftPeerId.valueOf(toRaftPeerIdString(id));
  }

  static RaftPeer toRaftPeer(DatanodeID id) {
    return new RaftPeer(toRaftPeerId(id), toRaftPeerAddressString(id));
  }

  static List<RaftPeer> toRaftPeers(Pipeline pipeline) {
    return toRaftPeers(pipeline.getMachines());
  }

  static <E extends DatanodeID> List<RaftPeer> toRaftPeers(List<E> datanodes) {
    return datanodes.stream().map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
  }

  /* TODO: use a dummy id for all groups for the moment.
   *       It should be changed to a unique id for each group.
   */
  RaftGroupId DUMMY_GROUP_ID =
      RaftGroupId.valueOf(ByteString.copyFromUtf8("AOzoneRatisGroup"));

  RaftGroup EMPTY_GROUP = new RaftGroup(DUMMY_GROUP_ID,
      Collections.emptyList());

  static RaftGroup emptyRaftGroup() {
    return EMPTY_GROUP;
  }

  static RaftGroup newRaftGroup(List<DatanodeID> datanodes) {
    final List<RaftPeer> newPeers = datanodes.stream()
        .map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
    return RatisHelper.newRaftGroup(newPeers);
  }

  static RaftGroup newRaftGroup(Collection<RaftPeer> peers) {
    return peers.isEmpty()? emptyRaftGroup()
        : new RaftGroup(DUMMY_GROUP_ID, peers);
  }

  static RaftGroup newRaftGroup(Pipeline pipeline) {
    return newRaftGroup(toRaftPeers(pipeline));
  }

  static RaftClient newRaftClient(RpcType rpcType, Pipeline pipeline) {
    return newRaftClient(rpcType, toRaftPeerId(pipeline.getLeader()),
        newRaftGroup(pipeline));
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader) {
    return newRaftClient(rpcType, leader.getId(),
        newRaftGroup(new ArrayList<>(Arrays.asList(leader))));
  }

  static RaftClient newRaftClient(
      RpcType rpcType, RaftPeerId leader, RaftGroup group) {
    LOG.trace("newRaftClient: {}, leader={}, group={}", rpcType, leader, group);
    final RaftProperties properties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(properties, rpcType);

    return RaftClient.newBuilder()
        .setRaftGroup(group)
        .setLeaderId(leader)
        .setProperties(properties)
        .build();
  }
}
