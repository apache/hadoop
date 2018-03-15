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

package org.apache.hadoop.ozone.container.common.transport.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;

/**
 * Netty server handlers that respond to Network events.
 */
public class XceiverServerHandler extends
    SimpleChannelInboundHandler<ContainerCommandRequestProto> {

  static final Logger LOG = LoggerFactory.getLogger(XceiverServerHandler.class);
  private final ContainerDispatcher dispatcher;

  /**
   * Constructor for server handler.
   * @param dispatcher - Dispatcher interface
   */
  public XceiverServerHandler(ContainerDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  /**
   * <strong>Please keep in mind that this method will be renamed to {@code
   * messageReceived(ChannelHandlerContext, I)} in 5.0.</strong>
   * <p>
   * Is called for each message of type {@link ContainerCommandRequestProto}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this {@link
   *            SimpleChannelInboundHandler} belongs to
   * @param msg the message to handle
   * @throws Exception is thrown if an error occurred
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx,
                           ContainerCommandRequestProto msg) throws
      Exception {
    ContainerCommandResponseProto response = this.dispatcher.dispatch(msg);
    LOG.debug("Writing the reponse back to client.");
    ctx.writeAndFlush(response);

  }

  /**
   * Calls {@link ChannelHandlerContext#fireExceptionCaught(Throwable)}
   * Sub-classes may override this method to change behavior.
   *
   * @param ctx   - Channel Handler Context
   * @param cause - Exception
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    LOG.error("An exception caught in the pipeline : " + cause.toString());
    super.exceptionCaught(ctx, cause);
  }
}
