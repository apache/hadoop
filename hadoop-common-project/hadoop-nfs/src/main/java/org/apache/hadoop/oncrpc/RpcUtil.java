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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    ctx.fireChannelRead(response);
  }

  public static ByteToMessageDecoder constructRpcFrameDecoder() {
    return new RpcFrameDecoder();
  }

  public static final ChannelInboundHandlerAdapter STAGE_RPC_MESSAGE_PARSER = new RpcMessageParserStage();
  public static final ChannelInboundHandlerAdapter STAGE_RPC_TCP_RESPONSE = new RpcTcpResponseStage();
  public static final ChannelInboundHandlerAdapter STAGE_RPC_UDP_RESPONSE = new RpcUdpResponseStage();

  /**
   * An RPC client can separate a RPC message into several frames (i.e.,
   * fragments) when transferring it across the wire. RpcFrameDecoder
   * reconstructs a full RPC message from these fragments.
   *
   * RpcFrameDecoder is a stateful pipeline stage. It has to be constructed for
   * each RPC client.
   */
  static class RpcFrameDecoder extends ByteToMessageDecoder {
    public static final Logger LOG =
        LoggerFactory.getLogger(RpcFrameDecoder.class);
    private volatile boolean isLast;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf,
        List<Object> out) {

      if (buf.readableBytes() < 4) {
        return;
      }

      buf.markReaderIndex();

      byte[] fragmentHeader = new byte[4];
      buf.readBytes(fragmentHeader);
      int length = XDR.fragmentSize(fragmentHeader);
      isLast = XDR.isLastFragment(fragmentHeader);

      if (buf.readableBytes() < length) {
        buf.resetReaderIndex();
        return;
      }

      ByteBuf newFragment = buf.readSlice(length);
      newFragment.retain();
      out.add(newFragment);
    }

    @VisibleForTesting
    public boolean isLast() {
      return isLast;
    }
  }

  /**
   * RpcMessageParserStage parses the network bytes and encapsulates the RPC
   * request into a RpcInfo instance.
   */
  @ChannelHandler.Sharable
  static final class RpcMessageParserStage extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory
        .getLogger(RpcMessageParserStage.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
      ByteBuf buf;
      SocketAddress remoteAddress;
      if (msg instanceof DatagramPacket) {
        DatagramPacket packet = (DatagramPacket)msg;
        buf = packet.content();
        remoteAddress = packet.sender();
      } else {
        buf = (ByteBuf) msg;
        remoteAddress = ctx.channel().remoteAddress();
      }

      ByteBuffer b = buf.nioBuffer().asReadOnlyBuffer();
      XDR in = new XDR(b, XDR.State.READING);

      RpcInfo info = null;
      try {
        RpcCall callHeader = RpcCall.read(in);
        ByteBuf dataBuffer = buf.slice(b.position(), b.remaining());

        info = new RpcInfo(callHeader, dataBuffer, ctx, ctx.channel(),
            remoteAddress);
      } catch (Exception exc) {
        LOG.info("Malformed RPC request from " + remoteAddress);
      } finally {
        // only release buffer if it is not passed to downstream handler
        if (info == null) {
          buf.release();
        }
      }

      if (info != null) {
        ctx.fireChannelRead(info);
      }
    }
  }

  /**
   * RpcTcpResponseStage sends an RpcResponse across the wire with the
   * appropriate fragment header.
   */
  @ChannelHandler.Sharable
  private static class RpcTcpResponseStage extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
      RpcResponse r = (RpcResponse) msg;
      byte[] fragmentHeader = XDR.recordMark(r.data().readableBytes(), true);
      ByteBuf header = Unpooled.wrappedBuffer(fragmentHeader);
      ByteBuf d = Unpooled.wrappedBuffer(header, r.data());
      ctx.channel().writeAndFlush(d);
    }
  }

  /**
   * RpcUdpResponseStage sends an RpcResponse as a UDP packet, which does not
   * require a fragment header.
   */
  @ChannelHandler.Sharable
  private static final class RpcUdpResponseStage extends
          SimpleChannelInboundHandler<RpcResponse> {
    public RpcUdpResponseStage() {
      // do not auto release the RpcResponse message.
      super(false);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx,
                                RpcResponse response) throws Exception {
      ByteBuf buf = Unpooled.wrappedBuffer(response.data());
      ctx.writeAndFlush(new DatagramPacket(
              buf, (InetSocketAddress) response.recipient()));
    }
  }
}
