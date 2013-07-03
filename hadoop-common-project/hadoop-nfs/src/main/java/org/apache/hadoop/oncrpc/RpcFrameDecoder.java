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

/**
 * {@link FrameDecoder} for RPC messages.
 */
public class RpcFrameDecoder extends FrameDecoder {
  public static final Log LOG = LogFactory.getLog(RpcFrameDecoder.class);
  private ChannelBuffer frame;

  /**
   * Decode an RPC message received on the socket.
   * @return mpnull if incomplete message is received.
   */
  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel,
      ChannelBuffer buf) {

    // Make sure if the length field was received.
    if (buf.readableBytes() < 4) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Length field is not received yet");
      }
      return null;
    }

    // Note the index and go back to it when an incomplete message is received
    buf.markReaderIndex();

    // Read the record marking.
    ChannelBuffer fragmentHeader = buf.readBytes(4);
    int length = XDR.fragmentSize(fragmentHeader.array());
    boolean isLast = XDR.isLastFragment(fragmentHeader.array());

    // Make sure if there's enough bytes in the buffer.
    if (buf.readableBytes() < length) {

      if (LOG.isTraceEnabled()) {
        LOG.trace(length + " bytes are not received yet");
      }
      buf.resetReaderIndex(); // Go back to the right reader index
      return null;
    }

    if (frame == null) {
      frame = buf.readBytes(length);
    } else {
      ChannelBuffer tmp = ChannelBuffers.copiedBuffer(frame.array(), buf
          .readBytes(length).array());
      frame = tmp;
    }

    // Successfully decoded a frame. Return the decoded frame if the frame is
    // the last one. Otherwise, wait for the next frame.
    if (isLast) {
      ChannelBuffer completeFrame = frame;
      frame = null;
      return completeFrame;
    } else {
      LOG.info("Wait for the next frame. This rarely happens.");
      return null;
    }
  }
}