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
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceStub;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.util.Time;
import org.apache.ratis.shaded.io.grpc.ManagedChannel;
import org.apache.ratis.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.shaded.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A Client for the storageContainer protocol.
 */
public class XceiverClientGrpc extends XceiverClientSpi {
  static final Logger LOG = LoggerFactory.getLogger(XceiverClientGrpc.class);
  private final Pipeline pipeline;
  private final Configuration config;
  private XceiverClientProtocolServiceStub asyncStub;
  private XceiverClientMetrics metrics;
  private ManagedChannel channel;
  private final Semaphore semaphore;
  private boolean closed = false;

  /**
   * Constructs a client that can communicate with the Container framework on
   * data nodes.
   *
   * @param pipeline - Pipeline that defines the machines.
   * @param config -- Ozone Config
   */
  public XceiverClientGrpc(Pipeline pipeline, Configuration config) {
    super();
    Preconditions.checkNotNull(pipeline);
    Preconditions.checkNotNull(config);
    this.pipeline = pipeline;
    this.config = config;
    this.semaphore =
        new Semaphore(HddsClientUtils.getMaxOutstandingRequests(config));
    this.metrics = XceiverClientManager.getXceiverClientMetrics();
  }

  @Override
  public void connect() throws Exception {
    DatanodeDetails leader = this.pipeline.getLeader();

    // read port from the data node, on failure use default configured
    // port.
    int port = leader.getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
    if (port == 0) {
      port = config.getInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT);
    }
    LOG.debug("Connecting to server Port : " + leader.getIpAddress());
    channel = NettyChannelBuilder.forAddress(leader.getIpAddress(), port)
        .usePlaintext()
        .maxInboundMessageSize(OzoneConfigKeys.DFS_CONTAINER_CHUNK_MAX_SIZE)
        .build();
    asyncStub = XceiverClientProtocolServiceGrpc.newStub(channel);
  }

  /**
   * Returns if the xceiver client connects to a server.
   *
   * @return True if the connection is alive, false otherwise.
   */
  @VisibleForTesting
  public boolean isConnected() {
    return !channel.isTerminated() && !channel.isShutdown();
  }

  @Override
  public void close() {
    closed = true;
    channel.shutdownNow();
    try {
      channel.awaitTermination(60, TimeUnit.MINUTES);
    } catch (Exception e) {
      LOG.error("Unexpected exception while waiting for channel termination",
          e);
    }
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @return Response to the command
   * @throws IOException
   */
  @Override
  public CompletableFuture<ContainerCommandResponseProto>
      sendCommandAsync(ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException {
    if(closed){
      throw new IOException("This channel is not connected.");
    }

    if(channel == null || !isConnected()) {
      reconnect();
    }

    final CompletableFuture<ContainerCommandResponseProto> replyFuture =
        new CompletableFuture<>();
    semaphore.acquire();
    long requestTime = Time.monotonicNowNanos();
    metrics.incrPendingContainerOpsMetrics(request.getCmdType());
    // create a new grpc stream for each non-async call.
    final StreamObserver<ContainerCommandRequestProto> requestObserver =
        asyncStub.send(new StreamObserver<ContainerCommandResponseProto>() {
          @Override
          public void onNext(ContainerCommandResponseProto value) {
            replyFuture.complete(value);
            metrics.decrPendingContainerOpsMetrics(request.getCmdType());
            metrics.addContainerOpsLatency(request.getCmdType(),
                Time.monotonicNowNanos() - requestTime);
            semaphore.release();
          }
          @Override
          public void onError(Throwable t) {
            replyFuture.completeExceptionally(t);
            metrics.decrPendingContainerOpsMetrics(request.getCmdType());
            metrics.addContainerOpsLatency(request.getCmdType(),
                Time.monotonicNowNanos() - requestTime);
            semaphore.release();
          }

          @Override
          public void onCompleted() {
            if (!replyFuture.isDone()) {
              replyFuture.completeExceptionally(
                  new IOException("Stream completed but no reply for request "
                      + request));
            }
          }
        });
    requestObserver.onNext(request);
    requestObserver.onCompleted();
    return replyFuture;
  }

  private void reconnect() throws IOException {
    try {
      connect();
    } catch (Exception e) {
      LOG.error("Error while connecting: ", e);
      throw new IOException(e);
    }

    if (channel == null || !isConnected()) {
      throw new IOException("This channel is not connected.");
    }
  }

  /**
   * Create a pipeline.
   */
  @Override
  public void createPipeline() {
    // For stand alone pipeline, there is no notion called setup pipeline.
  }

  public void destroyPipeline() {
    // For stand alone pipeline, there is no notion called destroy pipeline.
  }

  /**
   * Returns pipeline Type.
   *
   * @return - Stand Alone as the type.
   */
  @Override
  public HddsProtos.ReplicationType getPipelineType() {
    return HddsProtos.ReplicationType.STAND_ALONE;
  }
}
