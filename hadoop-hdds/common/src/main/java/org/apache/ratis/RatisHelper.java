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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Ratis helper methods.
 */
public interface RatisHelper {
  Logger LOG = LoggerFactory.getLogger(RatisHelper.class);

  static String toRaftPeerIdString(DatanodeDetails id) {
    return id.getUuidString();
  }

  static UUID toDatanodeId(String peerIdString) {
    return UUID.fromString(peerIdString);
  }

  static UUID toDatanodeId(RaftPeerId peerId) {
    return toDatanodeId(peerId.toString());
  }

  static UUID toDatanodeId(RaftProtos.RaftPeerProto peerId) {
    return toDatanodeId(RaftPeerId.valueOf(peerId.getId()));
  }

  static String toRaftPeerAddressString(DatanodeDetails id) {
    return id.getIpAddress() + ":" +
        id.getPort(DatanodeDetails.Port.Name.RATIS).getValue();
  }

  static RaftPeerId toRaftPeerId(DatanodeDetails id) {
    return RaftPeerId.valueOf(toRaftPeerIdString(id));
  }

  static RaftPeer toRaftPeer(DatanodeDetails id) {
    return new RaftPeer(toRaftPeerId(id), toRaftPeerAddressString(id));
  }

  static List<RaftPeer> toRaftPeers(Pipeline pipeline) {
    return toRaftPeers(pipeline.getNodes());
  }

  static <E extends DatanodeDetails> List<RaftPeer> toRaftPeers(
      List<E> datanodes) {
    return datanodes.stream().map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
  }

  /* TODO: use a dummy id for all groups for the moment.
   *       It should be changed to a unique id for each group.
   */
  RaftGroupId DUMMY_GROUP_ID =
      RaftGroupId.valueOf(ByteString.copyFromUtf8("AOzoneRatisGroup"));

  RaftGroup EMPTY_GROUP = RaftGroup.valueOf(DUMMY_GROUP_ID,
      Collections.emptyList());

  static RaftGroup emptyRaftGroup() {
    return EMPTY_GROUP;
  }

  static RaftGroup newRaftGroup(Collection<RaftPeer> peers) {
    return peers.isEmpty()? emptyRaftGroup()
        : RaftGroup.valueOf(DUMMY_GROUP_ID, peers);
  }

  static RaftGroup newRaftGroup(RaftGroupId groupId,
      Collection<DatanodeDetails> peers) {
    final List<RaftPeer> newPeers = peers.stream()
        .map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
    return peers.isEmpty() ? RaftGroup.valueOf(groupId, Collections.emptyList())
        : RaftGroup.valueOf(groupId, newPeers);
  }

  static RaftGroup newRaftGroup(Pipeline pipeline) {
    return RaftGroup.valueOf(RaftGroupId.valueOf(pipeline.getId().getId()),
        toRaftPeers(pipeline));
  }

  static RaftClient newRaftClient(RpcType rpcType, Pipeline pipeline,
      RetryPolicy retryPolicy) throws IOException {
    return newRaftClient(rpcType, toRaftPeerId(pipeline.getFirstNode()),
        newRaftGroup(RaftGroupId.valueOf(pipeline.getId().getId()),
            pipeline.getNodes()), retryPolicy);
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RetryPolicy retryPolicy) {
    return newRaftClient(rpcType, leader.getId(),
        newRaftGroup(new ArrayList<>(Arrays.asList(leader))), retryPolicy);
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RaftGroup group, RetryPolicy retryPolicy) {
    return newRaftClient(rpcType, leader.getId(), group, retryPolicy);
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeerId leader,
      RaftGroup group, RetryPolicy retryPolicy) {
    LOG.trace("newRaftClient: {}, leader={}, group={}", rpcType, leader, group);
    final RaftProperties properties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(properties, rpcType);

    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE));

    return RaftClient.newBuilder()
        .setRaftGroup(group)
        .setLeaderId(leader)
        .setProperties(properties)
        .setRetryPolicy(retryPolicy)
        .build();
  }

  static RetryPolicy createRetryPolicy(Configuration conf) {
    int maxRetryCount =
        conf.getInt(OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY,
            OzoneConfigKeys.
                DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_DEFAULT);
    long retryInterval = conf.getTimeDuration(OzoneConfigKeys.
        DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_KEY, OzoneConfigKeys.
        DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_DEFAULT
        .toIntExact(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    TimeDuration sleepDuration =
        TimeDuration.valueOf(retryInterval, TimeUnit.MILLISECONDS);
    RetryPolicy retryPolicy = RetryPolicies
        .retryUpToMaximumCountWithFixedSleep(maxRetryCount, sleepDuration);
    return retryPolicy;
  }
}
