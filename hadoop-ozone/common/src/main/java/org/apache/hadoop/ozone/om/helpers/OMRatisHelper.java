/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ratis helper methods for OM Ratis server and client.
 */
public final class OMRatisHelper {
  private static final Logger LOG = LoggerFactory.getLogger(
      OMRatisHelper.class);

  private OMRatisHelper() {
  }

  /**
   * Creates a new RaftClient object.
   *
   * @param rpcType     Replication Type
   * @param omId        OM id of the client
   * @param group       RaftGroup
   * @param retryPolicy Retry policy
   * @return RaftClient object
   */
  public static RaftClient newRaftClient(RpcType rpcType, String omId, RaftGroup
      group, RetryPolicy retryPolicy, Configuration conf) {
    LOG.trace("newRaftClient: {}, leader={}, group={}", rpcType, omId, group);
    final RaftProperties properties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(properties, rpcType);

    final int raftSegmentPreallocatedSize = (int) conf.getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT,
        StorageUnit.BYTES);
    GrpcConfigKeys.setMessageSizeMax(
        properties, SizeInBytes.valueOf(raftSegmentPreallocatedSize));

    return RaftClient.newBuilder()
        .setRaftGroup(group)
        .setLeaderId(getRaftPeerId(omId))
        .setProperties(properties)
        .setRetryPolicy(retryPolicy)
        .build();
  }

  static RaftPeerId getRaftPeerId(String omId) {
    return RaftPeerId.valueOf(omId);
  }

  public static ByteString convertRequestToByteString(OMRequest request) {
    byte[] requestBytes = request.toByteArray();
    return ByteString.copyFrom(requestBytes);
  }

  public static OMRequest convertByteStringToOMRequest(ByteString byteString)
      throws InvalidProtocolBufferException {
    byte[] bytes = byteString.toByteArray();
    return OMRequest.parseFrom(bytes);
  }

  public static Message convertResponseToMessage(OMResponse response) {
    byte[] requestBytes = response.toByteArray();
    return Message.valueOf(ByteString.copyFrom(requestBytes));
  }

  public static OMResponse getOMResponseFromRaftClientReply(
      RaftClientReply reply) throws InvalidProtocolBufferException {
    byte[] bytes = reply.getMessage().getContent().toByteArray();
    return OMResponse.newBuilder(OMResponse.parseFrom(bytes))
        .setLeaderOMNodeId(reply.getReplierId())
        .build();
  }
}