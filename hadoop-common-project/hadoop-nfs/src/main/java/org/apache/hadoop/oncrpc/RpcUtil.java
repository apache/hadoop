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

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public final class RpcUtil {
  /**
   * The XID in RPC call. It is used for starting with new seed after each
   * reboot.
   */
  private static int xid = (int) (System.currentTimeMillis() / 1000) << 12;

  public static int getNewXid(String caller) {
    return xid = ++xid + caller.hashCode();
  }

  public static void sendRpcResponse(ChannelHandlerContext ctx,
      RpcResponse response) {
    Channels.fireMessageReceived(ctx, response);
  }

  public static FrameDecoder constructRpcFrameDecoder() {
    return new RpcFrameDecoder();
  }

  public static final SimpleChannelUpstreamHandler STAGE_RPC_MESSAGE_PARSER = new RpcMessageParserStage();
  public static final SimpleChannelUpstreamHandler STAGE_RPC_TCP_RESPONSE = new RpcTcpResponseStage();
  public static final SimpleChannelUpstreamHandler STAGE_RPC_UDP_RESPONSE = new RpcUdpResponseStage();

  /**
   * An RPC client can separate a RPC message into several frames (i.e.,
   * fragments) when transferring it across the wire. RpcFrameDecoder
   * reconstructs a full RPC message from these fragments.
   *
   * RpcFrameDecoder is a stateful pipeline stage. It has to be constructed for
   * each RPC client.
   */
  static class RpcFrameDecoder extends FrameDecoder {
    public static final Log LOG = LogFactory.getLog(RpcFrameDecoder.class);
    private ChannelBuffer currentFrame;

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel,
        ChannelBuffer buf) {

      if (buf.readableBytes() < 4)
        return null;

      buf.markReaderIndex();

      byte[] fragmentHeader = new byte[4];
      buf.readBytes(fragmentHeader);
      int length = XDR.fragmentSize(fragmentHeader);
      boolean isLast = XDR.isLastFragment(fragmentHeader);

      if (buf.readableBytes() < length) {
        buf.resetReaderIndex();
        return null;
      }

      ChannelBuffer newFragment = buf.readSlice(length);
      if (currentFrame == null) {
        currentFrame = newFragment;
      } else {
        currentFrame = ChannelBuffers.wrappedBuffer(currentFrame, newFragment);
      }

      if (isLast) {
        ChannelBuffer completeFrame = currentFrame;
        currentFrame = null;
        return completeFrame;
      } else {
        return null;
      }
    }
  }

  /**
   * RpcMessageParserStage parses the network bytes and encapsulates the RPC
   * request into a RpcInfo instance.
   */
  static final class RpcMessageParserStage extends SimpleChannelUpstreamHandler {
    private static final Log LOG = LogFactory
        .getLog(RpcMessageParserStage.class);

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {
      ChannelBuffer buf = (ChannelBuffer) e.getMessage();
      ByteBuffer b = buf.toByteBuffer().asReadOnlyBuffer();
      XDR in = new XDR(b, XDR.State.READING);

      RpcInfo info = null;
      try {
        RpcCall callHeader = RpcCall.read(in);
        ChannelBuffer dataBuffer = ChannelBuffers.wrappedBuffer(in.buffer()
            .slice());
        info = new RpcInfo(callHeader, dataBuffer, ctx, e.getChannel(),
            e.getRemoteAddress());
      } catch (Exception exc) {
        LOG.info("Malformed RPC request from " + e.getRemoteAddress());
      }

      if (info != null) {
        Channels.fireMessageReceived(ctx, info);
      }
    }
  }

  /**
   * RpcTcpResponseStage sends an RpcResponse across the wire with the
   * appropriate fragment header.
   */
  private static class RpcTcpResponseStage extends SimpleChannelUpstreamHandler {

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {
      RpcResponse r = (RpcResponse) e.getMessage();
      byte[] fragmentHeader = XDR.recordMark(r.data().readableBytes(), true);
      ChannelBuffer header = ChannelBuffers.wrappedBuffer(fragmentHeader);
      ChannelBuffer d = ChannelBuffers.wrappedBuffer(header, r.data());
      e.getChannel().write(d);
    }
  }

  /**
   * RpcUdpResponseStage sends an RpcResponse as a UDP packet, which does not
   * require a fragment header.
   */
  private static final class RpcUdpResponseStage extends
      SimpleChannelUpstreamHandler {

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {
      RpcResponse r = (RpcResponse) e.getMessage();
      e.getChannel().write(r.data(), r.remoteAddress());
    }
  }
}
