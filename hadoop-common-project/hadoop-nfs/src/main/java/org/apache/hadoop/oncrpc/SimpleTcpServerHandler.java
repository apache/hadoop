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
package org.apache.hadoop.oncrpc;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * Handler used by {@link SimpleTcpServer}.
 */
public class SimpleTcpServerHandler extends SimpleChannelHandler {
  public static final Log LOG = LogFactory.getLog(SimpleTcpServerHandler.class);

  protected final RpcProgram rpcProgram;

  public SimpleTcpServerHandler(RpcProgram rpcProgram) {
    this.rpcProgram = rpcProgram;
  }
  
  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
    ChannelBuffer buf = (ChannelBuffer) e.getMessage();
    XDR request = new XDR(buf.toByteBuffer().asReadOnlyBuffer(), XDR.State.READING);
    
    InetAddress remoteInetAddr = ((InetSocketAddress) ctx.getChannel()
        .getRemoteAddress()).getAddress();
    Channel outChannel = e.getChannel();
    XDR response = rpcProgram.handle(request, remoteInetAddr, outChannel);
    if (response.size() > 0) {
      outChannel.write(XDR.writeMessageTcp(response, true));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    LOG.warn("Encountered ", e.getCause());
    e.getChannel().close();
  }
}