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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.ratis.RatisHelper;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
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
    return new XceiverClientRatis(pipeline,
        SupportedRpcType.valueOfIgnoreCase(rpcType));
  }

  private final Pipeline pipeline;
  private final RpcType rpcType;
  private final AtomicReference<RaftClient> client = new AtomicReference<>();

  /** Constructs a client. */
  private XceiverClientRatis(Pipeline pipeline, RpcType rpcType) {
    super();
    this.pipeline = pipeline;
    this.rpcType = rpcType;
  }

  /**
   *  {@inheritDoc}
   */
  public void createPipeline(String clusterId, List<DatanodeID> datanodes)
      throws IOException {
    final RaftPeer[] newPeers = datanodes.stream().map(RatisHelper::toRaftPeer)
        .toArray(RaftPeer[]::new);
    LOG.debug("initializing pipeline:{} with nodes:{}", clusterId, newPeers);
    reinitialize(datanodes, newPeers);
  }

  /**
   * Returns Ratis as pipeline Type.
   * @return - Ratis
   */
  @Override
  public OzoneProtos.ReplicationType getPipelineType() {
    return OzoneProtos.ReplicationType.RATIS;
  }

  private void reinitialize(List<DatanodeID> datanodes, RaftPeer[] newPeers)
      throws IOException {
    if (datanodes.isEmpty()) {
      return;
    }

    IOException exception = null;
    for (DatanodeID d : datanodes) {
      try {
        reinitialize(d, newPeers);
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
   * @param datanode - new datanode
   * @param newPeers - Raft machines
   * @throws IOException - on Failure.
   */
  private void reinitialize(DatanodeID datanode, RaftPeer[] newPeers)
      throws IOException {
    final RaftPeer p = RatisHelper.toRaftPeer(datanode);
    try (RaftClient client = RatisHelper.newRaftClient(rpcType, p)) {
      client.reinitialize(newPeers, p.getId());
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
    throw new IOException("Not implemented");
  }
}
