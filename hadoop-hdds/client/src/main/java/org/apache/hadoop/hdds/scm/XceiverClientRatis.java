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

package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.thirdparty.com.google.protobuf
    .InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract implementation of {@link XceiverClientSpi} using Ratis.
 * The underlying RPC mechanism can be chosen via the constructor.
 */
public final class XceiverClientRatis extends XceiverClientSpi {
  static final Logger LOG = LoggerFactory.getLogger(XceiverClientRatis.class);

  public static XceiverClientRatis newXceiverClientRatis(
      org.apache.hadoop.hdds.scm.pipeline.Pipeline pipeline,
      Configuration ozoneConf) {
    final String rpcType = ozoneConf
        .get(ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
            ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final int maxOutstandingRequests =
        HddsClientUtils.getMaxOutstandingRequests(ozoneConf);
    final RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(ozoneConf);
    return new XceiverClientRatis(pipeline,
        SupportedRpcType.valueOfIgnoreCase(rpcType), maxOutstandingRequests,
        retryPolicy);
  }

  private final Pipeline pipeline;
  private final RpcType rpcType;
  private final AtomicReference<RaftClient> client = new AtomicReference<>();
  private final int maxOutstandingRequests;
  private final RetryPolicy retryPolicy;

  /**
   * Constructs a client.
   */
  private XceiverClientRatis(Pipeline pipeline, RpcType rpcType,
      int maxOutStandingChunks, RetryPolicy retryPolicy) {
    super();
    this.pipeline = pipeline;
    this.rpcType = rpcType;
    this.maxOutstandingRequests = maxOutStandingChunks;
    this.retryPolicy = retryPolicy;
  }

  /**
   * Returns Ratis as pipeline Type.
   *
   * @return - Ratis
   */
  @Override
  public HddsProtos.ReplicationType getPipelineType() {
    return HddsProtos.ReplicationType.RATIS;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void connect() throws Exception {
    LOG.debug("Connecting to pipeline:{} datanode:{}", getPipeline().getId(),
        RatisHelper.toRaftPeerId(pipeline.getFirstNode()));
    // TODO : XceiverClient ratis should pass the config value of
    // maxOutstandingRequests so as to set the upper bound on max no of async
    // requests to be handled by raft client
    if (!client.compareAndSet(null,
        RatisHelper.newRaftClient(rpcType, getPipeline(), retryPolicy))) {
      throw new IllegalStateException("Client is already connected.");
    }
  }

  @Override
  public void close() {
    final RaftClient c = client.getAndSet(null);
    if (c != null) {
      closeRaftClient(c);
    }
  }

  private void closeRaftClient(RaftClient raftClient) {
    try {
      raftClient.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private RaftClient getClient() {
    return Objects.requireNonNull(client.get(), "client is null");
  }

  private CompletableFuture<RaftClientReply> sendRequestAsync(
      ContainerCommandRequestProto request) {
    boolean isReadOnlyRequest = HddsUtils.isReadOnly(request);
    ByteString byteString = request.toByteString();
    LOG.debug("sendCommandAsync {} {}", isReadOnlyRequest, request);
    return isReadOnlyRequest ? getClient().sendReadOnlyAsync(() -> byteString) :
        getClient().sendAsync(() -> byteString);
  }

  @Override
  public void watchForCommit(long index, long timeout)
      throws InterruptedException, ExecutionException, TimeoutException,
      IOException {
    LOG.debug("commit index : {} watch timeout : {}", index, timeout);
    // create a new RaftClient instance for watch request
    RaftClient raftClient =
        RatisHelper.newRaftClient(rpcType, getPipeline(), retryPolicy);
    CompletableFuture<RaftClientReply> replyFuture = raftClient
        .sendWatchAsync(index, RaftProtos.ReplicationLevel.ALL_COMMITTED);
    try {
      replyFuture.get(timeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException toe) {
      LOG.warn("3 way commit failed ", toe);

      closeRaftClient(raftClient);
      // generate a new raft client instance again so that next watch request
      // does not get blocked for the previous one

      // TODO : need to remove the code to create the new RaftClient instance
      // here once the watch request bypassing sliding window in Raft Client
      // gets fixed.
      raftClient =
          RatisHelper.newRaftClient(rpcType, getPipeline(), retryPolicy);
      raftClient
          .sendWatchAsync(index, RaftProtos.ReplicationLevel.MAJORITY_COMMITTED)
          .get(timeout, TimeUnit.MILLISECONDS);
      LOG.info("Could not commit " + index + " to all the nodes."
          + "Committed by majority.");
    } finally {
      closeRaftClient(raftClient);
    }
  }
  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @return Response to the command
   */
  @Override
  public XceiverClientAsyncReply sendCommandAsync(
      ContainerCommandRequestProto request) {
    XceiverClientAsyncReply asyncReply = new XceiverClientAsyncReply(null);
    CompletableFuture<RaftClientReply> raftClientReply =
        sendRequestAsync(request);
    Collection<XceiverClientAsyncReply.CommitInfo> commitInfos =
        new ArrayList<>();
    CompletableFuture<ContainerCommandResponseProto> containerCommandResponse =
        raftClientReply.whenComplete((reply, e) -> LOG
            .debug("received reply {} for request: {} exception: {}", request,
                reply, e))
            .thenApply(reply -> {
              try {
                ContainerCommandResponseProto response =
                    ContainerCommandResponseProto
                        .parseFrom(reply.getMessage().getContent());
                reply.getCommitInfos().forEach(e -> {
                  XceiverClientAsyncReply.CommitInfo commitInfo =
                      new XceiverClientAsyncReply.CommitInfo(
                          e.getServer().getAddress(), e.getCommitIndex());
                  commitInfos.add(commitInfo);
                  asyncReply.setCommitInfos(commitInfos);
                  asyncReply.setLogIndex(reply.getLogIndex());
                });
                return response;
              } catch (InvalidProtocolBufferException e) {
                throw new CompletionException(e);
              }
            });
    asyncReply.setResponse(containerCommandResponse);
    return asyncReply;
  }

}
