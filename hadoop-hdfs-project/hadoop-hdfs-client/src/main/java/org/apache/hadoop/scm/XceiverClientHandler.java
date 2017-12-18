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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos
    .ContainerCommandResponseProto;

import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 * Netty client handler.
 */
public class XceiverClientHandler extends
    SimpleChannelInboundHandler<ContainerCommandResponseProto> {

  static final Logger LOG = LoggerFactory.getLogger(XceiverClientHandler.class);
  private final ConcurrentMap<String, ResponseFuture> responses =
      new ConcurrentHashMap<>();

  private final Pipeline pipeline;
  private volatile Channel channel;
  private XceiverClientMetrics metrics;
  private final Semaphore semaphore;

  /**
   * Constructs a client that can communicate to a container server.
   */
  public XceiverClientHandler(Pipeline pipeline, Semaphore semaphore) {
    super(false);
    Preconditions.checkNotNull(pipeline);
    this.pipeline = pipeline;
    this.metrics = XceiverClientManager.getXceiverClientMetrics();
    this.semaphore = semaphore;
  }

  /**
   * <strong>Please keep in mind that this method will be renamed to {@code
   * messageReceived(ChannelHandlerContext, I)} in 5.0.</strong>
   * <p>
   * Is called for each message of type {@link ContainerProtos
   * .ContainerCommandResponseProto}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this {@link
   * SimpleChannelInboundHandler} belongs to
   * @param msg the message to handle
   * @throws Exception is thrown if an error occurred
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx,
      ContainerProtos.ContainerCommandResponseProto msg)
      throws Exception {
    Preconditions.checkNotNull(msg);
    metrics.decrPendingContainerOpsMetrics(msg.getCmdType());

    String key = msg.getTraceID();
    ResponseFuture response = responses.remove(key);
    semaphore.release();

    if (response != null) {
      response.getFuture().complete(msg);

      long requestTime = response.getRequestTime();
      metrics.addContainerOpsLatency(msg.getCmdType(),
          Time.monotonicNowNanos() - requestTime);
    } else {
      LOG.error("A reply received for message that was not queued. trace " +
          "ID: {}", msg.getTraceID());
    }
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) {
    LOG.debug("channelRegistered: Connected to ctx");
    channel = ctx.channel();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.info("Exception in client " + cause.toString());
    Iterator<String> keyIterator = responses.keySet().iterator();
    while (keyIterator.hasNext()) {
      ResponseFuture response = responses.remove(keyIterator.next());
      response.getFuture().completeExceptionally(cause);
      semaphore.release();
    }
    ctx.close();
  }

  /**
   * Since netty is async, we send a work request and then wait until a response
   * appears in the reply queue. This is simple sync interface for clients. we
   * should consider building async interfaces for client if this turns out to
   * be a performance bottleneck.
   *
   * @param request - request.
   * @return -- response
   */

  public ContainerCommandResponseProto sendCommand(
      ContainerProtos.ContainerCommandRequestProto request)
      throws ExecutionException, InterruptedException {
    Future<ContainerCommandResponseProto> future = sendCommandAsync(request);
    return future.get();
  }

  /**
   * SendCommandAsyc queues a command to the Netty Subsystem and returns a
   * CompletableFuture. This Future is marked compeleted in the channelRead0
   * when the call comes back.
   * @param request - Request to execute
   * @return CompletableFuture
   */
  public CompletableFuture<ContainerCommandResponseProto> sendCommandAsync(
      ContainerProtos.ContainerCommandRequestProto request)
      throws InterruptedException {

    // Throw an exception of request doesn't have traceId
    if (StringUtils.isEmpty(request.getTraceID())) {
      throw new IllegalArgumentException("Invalid trace ID");
    }

    // Setting the datanode ID in the commands, so that we can distinguish
    // commands when the cluster simulator is running.
    if(!request.hasDatanodeID()) {
      throw new IllegalArgumentException("Invalid Datanode ID");
    }

    metrics.incrPendingContainerOpsMetrics(request.getCmdType());

    CompletableFuture<ContainerCommandResponseProto> future
        = new CompletableFuture<>();
    ResponseFuture response = new ResponseFuture(future,
        Time.monotonicNowNanos());
    semaphore.acquire();
    ResponseFuture previous = responses.putIfAbsent(
        request.getTraceID(), response);
    if (previous != null) {
      LOG.error("Command with Trace already exists. Ignoring this command. " +
              "{}. Previous Command: {}", request.getTraceID(),
          previous.toString());
      throw new IllegalStateException("Duplicate trace ID. Command with this " +
          "trace ID is already executing. Please ensure that " +
          "trace IDs are not reused. ID: " + request.getTraceID());
    }

    channel.writeAndFlush(request);
    return response.getFuture();
  }

  /**
   * Class wraps response future info.
   */
  static class ResponseFuture {
    private final long requestTime;
    private final CompletableFuture<ContainerCommandResponseProto> future;

    ResponseFuture(CompletableFuture<ContainerCommandResponseProto> future,
        long requestTime) {
      this.future = future;
      this.requestTime = requestTime;
    }

    public long getRequestTime() {
      return requestTime;
    }

    public CompletableFuture<ContainerCommandResponseProto> getFuture() {
      return future;
    }
  }
}
