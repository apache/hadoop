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
package org.apache.hadoop.ozone.container.common.transport.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Netty client handler.
 */
public class XceiverClientHandler extends
    SimpleChannelInboundHandler<ContainerProtos.ContainerCommandResponseProto> {

  static final Logger LOG = LoggerFactory.getLogger(XceiverClientHandler.class);
  private final BlockingQueue<ContainerProtos.ContainerCommandResponseProto>
      responses = new LinkedBlockingQueue<>();
  private final Pipeline pipeline;
  private volatile Channel channel;

  /**
   * Constructs a client that can communicate to a container server.
   */
  public XceiverClientHandler(Pipeline pipeline) {
    super(false);
    this.pipeline = pipeline;
  }

  /**
   * <strong>Please keep in mind that this method will be renamed to {@code
   * messageReceived(ChannelHandlerContext, I)} in 5.0.</strong>
   * <p>
   * Is called for each message of type {@link ContainerProtos
   * .ContainerCommandResponseProto}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this {@link
   *            SimpleChannelInboundHandler} belongs to
   * @param msg the message to handle
   * @throws Exception is thrown if an error occurred
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx,
                           ContainerProtos.ContainerCommandResponseProto msg)
      throws Exception {
    responses.add(msg);
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) {
    LOG.debug("channelRegistered: Connected to ctx");
    channel = ctx.channel();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.info("Exception in client " + cause.toString());
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
  public ContainerProtos.ContainerCommandResponseProto
      sendCommand(ContainerProtos.ContainerCommandRequestProto request) {

    ContainerProtos.ContainerCommandResponseProto response;
    channel.writeAndFlush(request);
    boolean interrupted = false;
    for (; ; ) {
      try {
        response = responses.take();
        break;
      } catch (InterruptedException ignore) {
        interrupted = true;
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return response;
  }

}
