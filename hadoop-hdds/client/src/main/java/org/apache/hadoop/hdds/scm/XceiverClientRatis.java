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
import org.apache.hadoop.io.MultipleIOException;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.shaded.com.google.protobuf
    .InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.util.CheckedBiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract implementation of {@link XceiverClientSpi} using Ratis.
 * The underlying RPC mechanism can be chosen via the constructor.
 */
public final class XceiverClientRatis extends XceiverClientSpi {
  static final Logger LOG = LoggerFactory.getLogger(XceiverClientRatis.class);

  public static XceiverClientRatis newXceiverClientRatis(
      Pipeline pipeline, Configuration ozoneConf) {
    final String rpcType = ozoneConf.get(
        ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
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
   * {@inheritDoc}
   */
  public void createPipeline() throws IOException {
    final RaftGroup group = RatisHelper.newRaftGroup(pipeline);
    LOG.debug("creating pipeline:{} with {}", pipeline.getId(), group);
    callRatisRpc(pipeline.getMachines(),
        (raftClient, peer) -> raftClient.groupAdd(group, peer.getId()));
  }

  /**
   * {@inheritDoc}
   */
  public void destroyPipeline() throws IOException {
    final RaftGroup group = RatisHelper.newRaftGroup(pipeline);
    LOG.debug("destroying pipeline:{} with {}", pipeline.getId(), group);
    callRatisRpc(pipeline.getMachines(), (raftClient, peer) -> raftClient
        .groupRemove(group.getGroupId(), true, peer.getId()));
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

  private void callRatisRpc(List<DatanodeDetails> datanodes,
      CheckedBiConsumer<RaftClient, RaftPeer, IOException> rpc)
      throws IOException {
    if (datanodes.isEmpty()) {
      return;
    }

    final List<IOException> exceptions =
        Collections.synchronizedList(new ArrayList<>());
    datanodes.parallelStream().forEach(d -> {
      final RaftPeer p = RatisHelper.toRaftPeer(d);
      try (RaftClient client = RatisHelper
          .newRaftClient(rpcType, p, retryPolicy)) {
        rpc.accept(client, p);
      } catch (IOException ioe) {
        exceptions.add(
            new IOException("Failed invoke Ratis rpc " + rpc + " for " + d,
                ioe));
      }
    });
    if (!exceptions.isEmpty()) {
      throw MultipleIOException.createIOException(exceptions);
    }
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void connect() throws Exception {
    LOG.debug("Connecting to pipeline:{} leader:{}",
        getPipeline().getId(),
        RatisHelper.toRaftPeerId(pipeline.getLeader()));
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
      try {
        c.close();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
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

  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @return Response to the command
   * @throws IOException
   */
  @Override
  public CompletableFuture<ContainerCommandResponseProto> sendCommandAsync(
      ContainerCommandRequestProto request) {
    return sendRequestAsync(request).whenComplete((reply, e) ->
          LOG.debug("received reply {} for request: {} exception: {}", request,
              reply, e))
        .thenApply(reply -> {
          try {
            return ContainerCommandResponseProto.parseFrom(
                reply.getMessage().getContent());
          } catch (InvalidProtocolBufferException e) {
            throw new CompletionException(e);
          }
        });
  }
}
