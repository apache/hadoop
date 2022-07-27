/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ipc.netty.server;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

@ChannelHandler.Sharable
public class NettyResponder extends ChannelOutboundHandlerAdapter
    implements Responder {
  private final Server server;

  public NettyResponder(Server server) {
    this.server = server;
  }

  @Override
  public void start() {
  }

  @Override
  public void interrupt() {
  }

  // called by handlers.
  // TODO: Is queuing required similar to the NioResponder implementation ?
  @Override
  public void doRespond(Server.RpcCall call) throws IOException {
    if (Server.LOG.isDebugEnabled()) {
      Server.LOG.debug(Thread.currentThread().getName() +
          ": responding to " + call);
    }
    NettyConnection connection = call.connection();
    io.netty.channel.Channel channel = connection.channel;
    channel.writeAndFlush(call);
  }

  // called by the netty context.  do not call externally.
  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
                    ChannelPromise promise) {
    if (msg instanceof Server.RpcCall) {
      Server.RpcCall call = (Server.RpcCall) msg;
      try {
        if (call.connection.useWrap) {
          server.wrapWithSasl(call);
        }
        byte[] response = call.rpcResponse.array();
        msg = Unpooled.wrappedBuffer(response);
        server.rpcMetrics.incrSentBytes(response.length);
        if (Server.LOG.isDebugEnabled()) {
          Server.LOG.debug(Thread.currentThread().getName() +
              ": responding to " + call +
              " Wrote " + response.length + " bytes.");
        }
      } catch (Throwable e) {
        Server.LOG.warn(Thread.currentThread().getName() +
            ", call " + call + ": output error");
        ctx.close();
        return;
      } finally {
        call.connection.decRpcCount();
      }
    }
    ctx.writeAndFlush(msg);
  }
}
