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

package org.apache.hadoop.scm;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.ShadedProtoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
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
        OzoneClientUtils.getMaxOutstandingRequests(ozoneConf);
    return new XceiverClientRatis(pipeline,
        SupportedRpcType.valueOfIgnoreCase(rpcType), maxOutstandingRequests);
  }

  private final Pipeline pipeline;
  private final RpcType rpcType;
  private final AtomicReference<RaftClient> client = new AtomicReference<>();
  private final int maxOutstandingRequests;

  /**
   * Constructs a client.
   */
  private XceiverClientRatis(Pipeline pipeline, RpcType rpcType,
      int maxOutStandingChunks) {
    super();
    this.pipeline = pipeline;
    this.rpcType = rpcType;
    this.maxOutstandingRequests = maxOutStandingChunks;
  }

  /**
   * {@inheritDoc}
   */
  public void createPipeline(String clusterId, List<DatanodeID> datanodes)
      throws IOException {
    RaftGroup group = RatisHelper.newRaftGroup(datanodes);
    LOG.debug("initializing pipeline:{} with nodes:{}", clusterId,
        group.getPeers());
    reinitialize(datanodes, group);
  }

  /**
   * Returns Ratis as pipeline Type.
   *
   * @return - Ratis
   */
  @Override
  public HdslProtos.ReplicationType getPipelineType() {
    return HdslProtos.ReplicationType.RATIS;
  }

  private void reinitialize(List<DatanodeID> datanodes, RaftGroup group)
      throws IOException {
    if (datanodes.isEmpty()) {
      return;
    }

    IOException exception = null;
    for (DatanodeID d : datanodes) {
      try {
        reinitialize(d, group);
      } catch (IOException ioe) {
        if (exception == null) {
          exception = new IOException(
              "Failed to reinitialize some of the RaftPeer(s)", ioe);
        } else {
          exception.addSuppressed(ioe);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

  /**
   * Adds a new peers to the Ratis Ring.
   *
   * @param datanode - new datanode
   * @param group    - Raft group
   * @throws IOException - on Failure.
   */
  private void reinitialize(DatanodeID datanode, RaftGroup group)
      throws IOException {
    final RaftPeer p = RatisHelper.toRaftPeer(datanode);
    try (RaftClient client = RatisHelper.newRaftClient(rpcType, p)) {
      client.reinitialize(group, p.getId());
    } catch (IOException ioe) {
      LOG.error("Failed to reinitialize RaftPeer:{} datanode: {}  ",
          p, datanode, ioe);
      throw new IOException("Failed to reinitialize RaftPeer " + p
          + "(datanode=" + datanode + ")", ioe);
    }
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void connect() throws Exception {
    LOG.debug("Connecting to pipeline:{} leader:{}",
        getPipeline().getPipelineName(),
        RatisHelper.toRaftPeerId(pipeline.getLeader()));
    // TODO : XceiverClient ratis should pass the config value of
    // maxOutstandingRequests so as to set the upper bound on max no of async
    // requests to be handled by raft client
    if (!client.compareAndSet(null,
        RatisHelper.newRaftClient(rpcType, getPipeline()))) {
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

  private boolean isReadOnly(ContainerCommandRequestProto proto) {
    switch (proto.getCmdType()) {
    case ReadContainer:
    case ReadChunk:
    case ListKey:
    case GetKey:
    case GetSmallFile:
    case ListContainer:
    case ListChunk:
      return true;
    case CloseContainer:
    case WriteChunk:
    case UpdateContainer:
    case CompactChunk:
    case CreateContainer:
    case DeleteChunk:
    case DeleteContainer:
    case DeleteKey:
    case PutKey:
    case PutSmallFile:
    default:
      return false;
    }
  }

  private RaftClientReply sendRequest(ContainerCommandRequestProto request)
      throws IOException {
    boolean isReadOnlyRequest = isReadOnly(request);
    ByteString byteString =
        ShadedProtoUtil.asShadedByteString(request.toByteArray());
    LOG.debug("sendCommand {} {}", isReadOnlyRequest, request);
    final RaftClientReply reply =  isReadOnlyRequest ?
        getClient().sendReadOnly(() -> byteString) :
        getClient().send(() -> byteString);
    LOG.debug("reply {} {}", isReadOnlyRequest, reply);
    return reply;
  }

  private CompletableFuture<RaftClientReply> sendRequestAsync(
      ContainerCommandRequestProto request) throws IOException {
    boolean isReadOnlyRequest = isReadOnly(request);
    ByteString byteString =
        ShadedProtoUtil.asShadedByteString(request.toByteArray());
    LOG.debug("sendCommandAsync {} {}", isReadOnlyRequest, request);
    return isReadOnlyRequest ? getClient().sendReadOnlyAsync(() -> byteString) :
        getClient().sendAsync(() -> byteString);
  }

  @Override
  public ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request) throws IOException {
    final RaftClientReply reply = sendRequest(request);
    Preconditions.checkState(reply.isSuccess());
    return ContainerCommandResponseProto.parseFrom(
        ShadedProtoUtil.asByteString(reply.getMessage().getContent()));
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
      ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException {
    return sendRequestAsync(request).whenComplete((reply, e) ->
          LOG.debug("received reply {} for request: {} exception: {}", request,
              reply, e))
        .thenApply(reply -> {
          try {
            return ContainerCommandResponseProto.parseFrom(
                ShadedProtoUtil.asByteString(reply.getMessage().getContent()));
          } catch (InvalidProtocolBufferException e) {
            throw new CompletionException(e);
          }
        });
  }
}
