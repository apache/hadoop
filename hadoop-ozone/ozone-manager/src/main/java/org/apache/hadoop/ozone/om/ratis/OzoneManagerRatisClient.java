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

package org.apache.hadoop.ozone.om.ratis;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftRetryFailureException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OM Ratis client to interact with OM Ratis server endpoint.
 */
public final class OzoneManagerRatisClient implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(
      OzoneManagerRatisClient.class);

  private final RaftGroup raftGroup;
  private final String omID;
  private final RpcType rpcType;
  private RaftClient raftClient;
  private final RetryPolicy retryPolicy;
  private final Configuration conf;

  private OzoneManagerRatisClient(String omId, RaftGroup raftGroup,
      RpcType rpcType, RetryPolicy retryPolicy,
      Configuration config) {
    this.raftGroup = raftGroup;
    this.omID = omId;
    this.rpcType = rpcType;
    this.retryPolicy = retryPolicy;
    this.conf = config;
  }

  public static OzoneManagerRatisClient newOzoneManagerRatisClient(
      String omId, RaftGroup raftGroup, Configuration conf) {
    final String rpcType = conf.get(
        OMConfigKeys.OZONE_OM_RATIS_RPC_TYPE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_RPC_TYPE_DEFAULT);

    final int maxRetryCount = conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY,
        OMConfigKeys.OZONE_OM_RATIS_CLIENT_REQUEST_MAX_RETRIES_DEFAULT);
    final long retryInterval = conf.getTimeDuration(
        OMConfigKeys.OZONE_OM_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_KEY,
        OMConfigKeys.OZONE_OM_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_DEFAULT
            .toIntExact(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    final TimeDuration sleepDuration = TimeDuration.valueOf(
        retryInterval, TimeUnit.MILLISECONDS);
    final RetryPolicy retryPolicy = RetryPolicies
        .retryUpToMaximumCountWithFixedSleep(maxRetryCount, sleepDuration);

    return new OzoneManagerRatisClient(omId, raftGroup,
        SupportedRpcType.valueOfIgnoreCase(rpcType), retryPolicy, conf);
  }

  public void connect() {
    LOG.debug("Connecting to OM Ratis Server GroupId:{} OM:{}",
        raftGroup.getGroupId().getUuid().toString(), omID);

    // TODO : XceiverClient ratis should pass the config value of
    // maxOutstandingRequests so as to set the upper bound on max no of async
    // requests to be handled by raft client

    raftClient = OMRatisHelper.newRaftClient(rpcType, omID, raftGroup,
        retryPolicy, conf);
  }

  @Override
  public void close() {
    if (raftClient != null) {
      try {
        raftClient.close();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /**
   * Sends a given request to server and gets the reply back.
   * @param request Request
   * @return Response to the command
   */
  public OMResponse sendCommand(OMRequest request) {
    try {
      CompletableFuture<OMResponse> reply = sendCommandAsync(request);
      return reply.get();
    } catch (ExecutionException | InterruptedException e) {
      LOG.error("Failed to execute command: " + request, e);
      return OMRatisHelper.getErrorResponse(request.getCmdType(), e);
    }
  }

  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @return Response to the command
   */
  private CompletableFuture<OMResponse> sendCommandAsync(OMRequest request) {
    CompletableFuture<RaftClientReply> raftClientReply =
        sendRequestAsync(request);

    CompletableFuture<OMResponse> omRatisResponse =
        raftClientReply.whenComplete((reply, e) -> LOG.debug(
            "received reply {} for request: cmdType={} traceID={} " +
                "exception: {}", reply, request.getCmdType(),
            request.getTraceID(), e))
            .thenApply(reply -> {
              try {
                // we need to handle RaftRetryFailure Exception
                RaftRetryFailureException raftRetryFailureException =
                    reply.getRetryFailureException();
                if (raftRetryFailureException != null) {
                  throw new CompletionException(raftRetryFailureException);
                }
                OMResponse response = OMRatisHelper
                    .convertByteStringToOMResponse(reply.getMessage()
                        .getContent());
                return response;
              } catch (InvalidProtocolBufferException e) {
                throw new CompletionException(e);
              }
            });
    return omRatisResponse;
  }

  /**
   * Submits {@link RaftClient#sendReadOnlyAsync(Message)} request to Ratis
   * server if the request is readOnly. Otherwise, submits
   * {@link RaftClient#sendAsync(Message)} request.
   * @param request OMRequest
   * @return RaftClient response
   */
  private CompletableFuture<RaftClientReply> sendRequestAsync(
      OMRequest request) {
    boolean isReadOnlyRequest = OmUtils.isReadOnly(request);
    ByteString byteString = OMRatisHelper.convertRequestToByteString(request);
    LOG.debug("sendOMRequestAsync {} {}", isReadOnlyRequest, request);
    return isReadOnlyRequest ? raftClient.sendReadOnlyAsync(() -> byteString) :
        raftClient.sendAsync(() -> byteString);
  }
}
