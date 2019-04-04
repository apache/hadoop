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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;

import io.opentracing.Scope;
import io.opentracing.util.GlobalTracer;
import org.apache.hadoop.util.Time;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftRetryFailureException;
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
import org.apache.hadoop.hdds.tracing.TracingUtil;

import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    final TimeDuration clientRequestTimeout =
        RatisHelper.getClientRequestTimeout(ozoneConf);
    final int maxOutstandingRequests =
        HddsClientUtils.getMaxOutstandingRequests(ozoneConf);
    final RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(ozoneConf);
    final GrpcTlsConfig tlsConfig = RatisHelper.createTlsClientConfig(new
        SecurityConfig(ozoneConf));
    return new XceiverClientRatis(pipeline,
        SupportedRpcType.valueOfIgnoreCase(rpcType), maxOutstandingRequests,
        retryPolicy, tlsConfig, clientRequestTimeout);
  }

  private final Pipeline pipeline;
  private final RpcType rpcType;
  private final AtomicReference<RaftClient> client = new AtomicReference<>();
  private final int maxOutstandingRequests;
  private final RetryPolicy retryPolicy;
  private final GrpcTlsConfig tlsConfig;
  private final TimeDuration clientRequestTimeout;

  // Map to track commit index at every server
  private final ConcurrentHashMap<UUID, Long> commitInfoMap;

  private XceiverClientMetrics metrics;

  /**
   * Constructs a client.
   */
  private XceiverClientRatis(Pipeline pipeline, RpcType rpcType,
      int maxOutStandingChunks, RetryPolicy retryPolicy,
      GrpcTlsConfig tlsConfig, TimeDuration timeout) {
    super();
    this.pipeline = pipeline;
    this.rpcType = rpcType;
    this.maxOutstandingRequests = maxOutStandingChunks;
    this.retryPolicy = retryPolicy;
    commitInfoMap = new ConcurrentHashMap<>();
    this.tlsConfig = tlsConfig;
    this.clientRequestTimeout = timeout;
    metrics = XceiverClientManager.getXceiverClientMetrics();
  }

  private void updateCommitInfosMap(
      Collection<RaftProtos.CommitInfoProto> commitInfoProtos) {
    // if the commitInfo map is empty, just update the commit indexes for each
    // of the servers
    if (commitInfoMap.isEmpty()) {
      commitInfoProtos.forEach(proto -> commitInfoMap
          .put(RatisHelper.toDatanodeId(proto.getServer()),
              proto.getCommitIndex()));
      // In case the commit is happening 2 way, just update the commitIndex
      // for the servers which have been successfully updating the commit
      // indexes. This is important because getReplicatedMinCommitIndex()
      // should always return the min commit index out of the nodes which have
      // been replicating data successfully.
    } else {
      commitInfoProtos.forEach(proto -> commitInfoMap
          .computeIfPresent(RatisHelper.toDatanodeId(proto.getServer()),
              (address, index) -> {
                index = proto.getCommitIndex();
                return index;
              }));
    }
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
        RatisHelper.newRaftClient(rpcType, getPipeline(), retryPolicy,
            maxOutstandingRequests, tlsConfig, clientRequestTimeout))) {
      throw new IllegalStateException("Client is already connected.");
    }
  }

  @Override
  public void connect(String encodedToken) throws Exception {
    throw new UnsupportedOperationException("Block tokens are not " +
        "implemented for Ratis clients.");
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


  @VisibleForTesting
  public ConcurrentHashMap<UUID, Long> getCommitInfoMap() {
    return commitInfoMap;
  }

  private CompletableFuture<RaftClientReply> sendRequestAsync(
      ContainerCommandRequestProto request) {
    try (Scope scope = GlobalTracer.get()
        .buildSpan("XceiverClientRatis." + request.getCmdType().name())
        .startActive(true)) {
      ContainerCommandRequestProto finalPayload =
          ContainerCommandRequestProto.newBuilder(request)
              .setTraceID(TracingUtil.exportCurrentSpan())
              .build();
      boolean isReadOnlyRequest = HddsUtils.isReadOnly(finalPayload);
      ByteString byteString = finalPayload.toByteString();
      LOG.debug("sendCommandAsync {} {}", isReadOnlyRequest, finalPayload);
      return isReadOnlyRequest ?
          getClient().sendReadOnlyAsync(() -> byteString) :
          getClient().sendAsync(() -> byteString);
    }
  }

  // gets the minimum log index replicated to all servers
  @Override
  public long getReplicatedMinCommitIndex() {
    OptionalLong minIndex =
        commitInfoMap.values().parallelStream().mapToLong(v -> v).min();
    return minIndex.isPresent() ? minIndex.getAsLong() : 0;
  }

  private void addDatanodetoReply(UUID address, XceiverClientReply reply) {
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(address.toString());
    reply.addDatanode(builder.build());
  }

  @Override
  public XceiverClientReply watchForCommit(long index, long timeout)
      throws InterruptedException, ExecutionException, TimeoutException,
      IOException {
    long commitIndex = getReplicatedMinCommitIndex();
    XceiverClientReply clientReply = new XceiverClientReply(null);
    if (commitIndex >= index) {
      // return the min commit index till which the log has been replicated to
      // all servers
      clientReply.setLogIndex(commitIndex);
      return clientReply;
    }
    LOG.debug("commit index : {} watch timeout : {}", index, timeout);
    CompletableFuture<RaftClientReply> replyFuture = getClient()
        .sendWatchAsync(index, RaftProtos.ReplicationLevel.ALL_COMMITTED);
    RaftClientReply reply;
    try {
      replyFuture.get(timeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException toe) {
      LOG.warn("3 way commit failed ", toe);
      reply = getClient()
          .sendWatchAsync(index, RaftProtos.ReplicationLevel.MAJORITY_COMMITTED)
          .get(timeout, TimeUnit.MILLISECONDS);
      List<RaftProtos.CommitInfoProto> commitInfoProtoList =
          reply.getCommitInfos().stream()
              .filter(i -> i.getCommitIndex() < index)
              .collect(Collectors.toList());
      commitInfoProtoList.parallelStream().forEach(proto -> {
        UUID address = RatisHelper.toDatanodeId(proto.getServer());
        addDatanodetoReply(address, clientReply);
        // since 3 way commit has failed, the updated map from now on  will
        // only store entries for those datanodes which have had successful
        // replication.
        commitInfoMap.remove(address);
        LOG.info(
            "Could not commit " + index + " to all the nodes. Server " + address
                + " has failed." + " Committed by majority.");
      });
    }
    clientReply.setLogIndex(index);
    return clientReply;
  }

  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @return Response to the command
   */
  @Override
  public XceiverClientReply sendCommandAsync(
      ContainerCommandRequestProto request) {
    XceiverClientReply asyncReply = new XceiverClientReply(null);
    long requestTime = Time.monotonicNowNanos();
    CompletableFuture<RaftClientReply> raftClientReply =
        sendRequestAsync(request);
    metrics.incrPendingContainerOpsMetrics(request.getCmdType());
    CompletableFuture<ContainerCommandResponseProto> containerCommandResponse =
        raftClientReply.whenComplete((reply, e) -> {
          LOG.debug("received reply {} for request: cmdType={} containerID={}"
                  + " pipelineID={} traceID={} exception: {}", reply,
              request.getCmdType(), request.getContainerID(),
              request.getPipelineID(), request.getTraceID(), e);
          metrics.decrPendingContainerOpsMetrics(request.getCmdType());
          metrics.addContainerOpsLatency(request.getCmdType(),
              Time.monotonicNowNanos() - requestTime);
        }).thenApply(reply -> {
          try {
            // we need to handle RaftRetryFailure Exception
            RaftRetryFailureException raftRetryFailureException =
                reply.getRetryFailureException();
            if (raftRetryFailureException != null) {
              // in case of raft retry failure, the raft client is
              // not able to connect to the leader hence the pipeline
              // can not be used but this instance of RaftClient will close
              // and refreshed again. In case the client cannot connect to
              // leader, getClient call will fail.

              // No need to set the failed Server ID here. Ozone client
              // will directly exclude this pipeline in next allocate block
              // to SCM as in this case, it is the raft client which is not
              // able to connect to leader in the pipeline, though the
              // pipeline can still be functional.
              throw new CompletionException(raftRetryFailureException);
            }
            ContainerCommandResponseProto response =
                ContainerCommandResponseProto
                    .parseFrom(reply.getMessage().getContent());
            UUID serverId = RatisHelper.toDatanodeId(reply.getReplierId());
            if (response.getResult() == ContainerProtos.Result.SUCCESS) {
              updateCommitInfosMap(reply.getCommitInfos());
            }
            asyncReply.setLogIndex(reply.getLogIndex());
            addDatanodetoReply(serverId, asyncReply);
            return response;
          } catch (InvalidProtocolBufferException e) {
            throw new CompletionException(e);
          }
        });
    asyncReply.setResponse(containerCommandResponse);
    return asyncReply;
  }

}
