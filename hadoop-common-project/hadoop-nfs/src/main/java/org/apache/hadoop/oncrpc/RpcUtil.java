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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class RpcUtil {
  /**
   * The XID in RPC call. It is used for starting with new seed after each reboot.
   */
  private static int xid = (int) (System.currentTimeMillis() / 1000) << 12;

  public static int getNewXid(String caller) {
    return xid = ++xid + caller.hashCode();
  }

  public static FrameDecoder constructRpcFrameDecoder() {
    return new RpcFrameDecoder();
  }

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
}
