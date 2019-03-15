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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      RetryPolicy retryPolicy, int maxOutStandingRequest,
      GrpcTlsConfig tlsConfig, TimeDuration timeout) throws IOException {
    return newRaftClient(rpcType, toRaftPeerId(pipeline.getFirstNode()),
        newRaftGroup(RaftGroupId.valueOf(pipeline.getId().getId()),
            pipeline.getNodes()), retryPolicy, maxOutStandingRequest, tlsConfig,
        timeout);
  }

  static TimeDuration getClientRequestTimeout(Configuration conf) {
    // Set the client requestTimeout
    final TimeUnit timeUnit =
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getUnit();
    final long duration = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_KEY,
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_TIMEOUT_DURATION_DEFAULT
            .getDuration(), timeUnit);
    final TimeDuration clientRequestTimeout =
        TimeDuration.valueOf(duration, timeUnit);
    return clientRequestTimeout;
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RetryPolicy retryPolicy, int maxOutstandingRequests,
      GrpcTlsConfig tlsConfig, TimeDuration clientRequestTimeout) {
    return newRaftClient(rpcType, leader.getId(),
        newRaftGroup(new ArrayList<>(Arrays.asList(leader))), retryPolicy,
        maxOutstandingRequests, tlsConfig, clientRequestTimeout);
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RetryPolicy retryPolicy, int maxOutstandingRequests,
      TimeDuration clientRequestTimeout) {
    return newRaftClient(rpcType, leader.getId(),
        newRaftGroup(new ArrayList<>(Arrays.asList(leader))), retryPolicy,
        maxOutstandingRequests, null, clientRequestTimeout);
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeerId leader,
      RaftGroup group, RetryPolicy retryPolicy, int maxOutStandingRequest,
      GrpcTlsConfig tlsConfig, TimeDuration clientRequestTimeout) {
    LOG.trace("newRaftClient: {}, leader={}, group={}", rpcType, leader, group);
    final RaftProperties properties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(properties, rpcType);
    RaftClientConfigKeys.Rpc
        .setRequestTimeout(properties, clientRequestTimeout);

    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE));
    GrpcConfigKeys.OutputStream.setOutstandingAppendsMax(properties,
        maxOutStandingRequest);

    RaftClient.Builder builder =  RaftClient.newBuilder()
        .setRaftGroup(group)
        .setLeaderId(leader)
        .setProperties(properties)
        .setRetryPolicy(retryPolicy);

    // TODO: GRPC TLS only for now, netty/hadoop RPC TLS support later.
    if (tlsConfig != null && rpcType == SupportedRpcType.GRPC) {
      builder.setParameters(GrpcFactory.newRaftParameters(tlsConfig));
    }
    return builder.build();
  }

  static GrpcTlsConfig createTlsClientConfig(SecurityConfig conf) {
    if (conf.isGrpcTlsEnabled()) {
      if (conf.isGrpcMutualTlsRequired()) {
        return new GrpcTlsConfig(
            null, null, conf.getTrustStoreFile(), false);
      } else {
        return new GrpcTlsConfig(conf.getClientPrivateKeyFile(),
            conf.getClientCertChainFile(), conf.getTrustStoreFile(), true);
      }
    }
    return null;
  }

  static GrpcTlsConfig createTlsServerConfig(SecurityConfig conf) {
    if (conf.isGrpcTlsEnabled()) {
      if (conf.isGrpcMutualTlsRequired()) {
        return new GrpcTlsConfig(
            conf.getServerPrivateKeyFile(), conf.getServerCertChainFile(), null,
            false);
      } else {
        return new GrpcTlsConfig(conf.getServerPrivateKeyFile(),
            conf.getServerCertChainFile(), conf.getClientCertChainFile(), true);
      }
    }
    return null;
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
