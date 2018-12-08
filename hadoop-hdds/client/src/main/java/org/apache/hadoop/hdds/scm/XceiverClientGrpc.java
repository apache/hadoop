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
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.datanode.proto.XceiverClientProtocolServiceGrpc.XceiverClientProtocolServiceStub;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.Time;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.UUID;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A Client for the storageContainer protocol.
 */
public class XceiverClientGrpc extends XceiverClientSpi {
  static final Logger LOG = LoggerFactory.getLogger(XceiverClientGrpc.class);
  private final Pipeline pipeline;
  private final Configuration config;
  private Map<UUID, XceiverClientProtocolServiceStub> asyncStubs;
  private XceiverClientMetrics metrics;
  private Map<UUID, ManagedChannel> channels;
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
    this.channels = new HashMap<>();
    this.asyncStubs = new HashMap<>();
  }

  @Override
  public void connect() throws Exception {

    // leader by default is the 1st datanode in the datanode list of pipleline
    DatanodeDetails dn = this.pipeline.getFirstNode();
    // just make a connection to the 1st datanode at the beginning
    connectToDatanode(dn);
  }

  private void connectToDatanode(DatanodeDetails dn) {
    // read port from the data node, on failure use default configured
    // port.
    int port = dn.getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
    if (port == 0) {
      port = config.getInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT);
    }
    LOG.debug("Connecting to server Port : " + dn.getIpAddress());
    ManagedChannel channel =
        NettyChannelBuilder.forAddress(dn.getIpAddress(), port).usePlaintext()
            .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
            .build();
    XceiverClientProtocolServiceStub asyncStub =
        XceiverClientProtocolServiceGrpc.newStub(channel);
    asyncStubs.put(dn.getUuid(), asyncStub);
    channels.put(dn.getUuid(), channel);
  }
  /**
   * Returns if the xceiver client connects to all servers in the pipeline.
   *
   * @return True if the connection is alive, false otherwise.
   */
  @VisibleForTesting
  public boolean isConnected(DatanodeDetails details) {
    return isConnected(channels.get(details.getUuid()));
  }

  private boolean isConnected(ManagedChannel channel) {
    return channel != null && !channel.isTerminated() && !channel.isShutdown();
  }

  @Override
  public void close() {
    closed = true;
    for (ManagedChannel channel : channels.values()) {
      channel.shutdownNow();
      try {
        channel.awaitTermination(60, TimeUnit.MINUTES);
      } catch (Exception e) {
        LOG.error("Unexpected exception while waiting for channel termination",
            e);
      }
    }
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public ContainerCommandResponseProto sendCommand(
      ContainerCommandRequestProto request) throws IOException {
    return sendCommandWithRetry(request);
  }

  public ContainerCommandResponseProto sendCommandWithRetry(
      ContainerCommandRequestProto request) throws IOException {
    ContainerCommandResponseProto responseProto = null;

    // In case of an exception or an error, we will try to read from the
    // datanodes in the pipeline in a round robin fashion.

    // TODO: cache the correct leader info in here, so that any subsequent calls
    // should first go to leader
    List<DatanodeDetails> dns = pipeline.getNodes();
    for (DatanodeDetails dn : dns) {
      try {
        LOG.debug("Executing command " + request + " on datanode " + dn);
        // In case the command gets retried on a 2nd datanode,
        // sendCommandAsyncCall will create a new channel and async stub
        // in case these don't exist for the specific datanode.
        responseProto = sendCommandAsync(request, dn).getResponse().get();
        if (responseProto.getResult() == ContainerProtos.Result.SUCCESS) {
          break;
        }
      } catch (ExecutionException | InterruptedException e) {
        LOG.warn("Failed to execute command " + request + " on datanode " + dn
            .getUuidString(), e);
      }
    }

    if (responseProto != null) {
      return responseProto;
    } else {
      throw new IOException(
          "Failed to execute command " + request + " on the pipeline "
              + pipeline.getId());
    }
  }

  // TODO: for a true async API, once the waitable future while executing
  // the command on one channel fails, it should be retried asynchronously
  // on the future Task for all the remaining datanodes.

  // Note: this Async api is not used currently used in any active I/O path.
  // In case it gets used, the asynchronous retry logic needs to be plugged
  // in here.
  /**
   * Sends a given command to server gets a waitable future back.
   *
   * @param request Request
   * @return Response to the command
   * @throws IOException
   */
  @Override
  public XceiverClientAsyncReply sendCommandAsync(
      ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException {
    XceiverClientAsyncReply asyncReply =
        sendCommandAsync(request, pipeline.getFirstNode());

    // TODO : for now make this API sync in nature as async requests are
    // served out of order over XceiverClientGrpc. This needs to be fixed
    // if this API is to be used for I/O path. Currently, this is not
    // used for Read/Write Operation but for tests.
    if (!HddsUtils.isReadOnly(request)) {
      asyncReply.getResponse().get();
    }
    return asyncReply;
  }

  private XceiverClientAsyncReply sendCommandAsync(
      ContainerCommandRequestProto request, DatanodeDetails dn)
      throws IOException, ExecutionException, InterruptedException {
    if (closed) {
      throw new IOException("This channel is not connected.");
    }

    UUID dnId = dn.getUuid();
    ManagedChannel channel = channels.get(dnId);
    // If the channel doesn't exist for this specific datanode or the channel
    // is closed, just reconnect
    if (!isConnected(channel)) {
      reconnect(dn);
    }

    final CompletableFuture<ContainerCommandResponseProto> replyFuture =
        new CompletableFuture<>();
    semaphore.acquire();
    long requestTime = Time.monotonicNowNanos();
    metrics.incrPendingContainerOpsMetrics(request.getCmdType());
    // create a new grpc stream for each non-async call.

    // TODO: for async calls, we should reuse StreamObserver resources.
    final StreamObserver<ContainerCommandRequestProto> requestObserver =
        asyncStubs.get(dnId)
            .send(new StreamObserver<ContainerCommandResponseProto>() {
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
                  replyFuture.completeExceptionally(new IOException(
                      "Stream completed but no reply for request " + request));
                }
              }
            });
    requestObserver.onNext(request);
    requestObserver.onCompleted();
    return new XceiverClientAsyncReply(replyFuture);
  }

  private void reconnect(DatanodeDetails dn)
      throws IOException {
    ManagedChannel channel;
    try {
      connectToDatanode(dn);
      channel = channels.get(dn.getUuid());
    } catch (Exception e) {
      LOG.error("Error while connecting: ", e);
      throw new IOException(e);
    }

    if (channel == null || !isConnected(channel)) {
      throw new IOException("This channel is not connected.");
    }
  }

  @Override
  public long watchForCommit(long index, long timeout)
      throws InterruptedException, ExecutionException, TimeoutException,
      IOException {
    // there is no notion of watch for commit index in standalone pipeline
    return 0;
  };

  public long getReplicatedMinCommitIndex() {
    return 0;
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
