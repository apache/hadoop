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
package org.apache.hadoop.hdfs.web.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2LocalFlowController;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.InternalThreadLocalMap;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.collect.ImmutableSet;

/**
 * A channel used for modeling an HTTP/2 stream.
 * <p>
 * We share the same event loop with the parent channel, so doBeginRead, doWrite
 * and doClose will run in the same event loop thread. So no event loop
 * switching is needed, and it is safe to call encoder.writeXXX directly in
 * doWrite.
 * <p>
 * But the public methods(isOpen, isActive...) can be called outside the event
 * loop, so the state field must be volatile.
 */
@InterfaceAudience.Private
public class Http2StreamChannel extends AbstractChannel {

  private static final ChannelMetadata METADATA = new ChannelMetadata(false);

  private static final int MAX_READER_STACK_DEPTH = 8;

  private final ChannelHandlerContext http2ConnHandlerCtx;

  private final Http2Stream stream;

  private final Http2LocalFlowController localFlowController;

  private final Http2ConnectionEncoder encoder;

  private final DefaultChannelConfig config;

  private static final class InboundMessage {

    public final Object msg;

    public final int length;

    public InboundMessage(Object msg, int length) {
      this.msg = msg;
      this.length = length;
    }

  }

  private final Queue<InboundMessage> inboundMessageQueue = new ArrayDeque<>();

  private boolean writePending = false;

  private int pendingOutboundBytes;

  private enum State {
    OPEN, HALF_CLOSED_LOCAL, HALF_CLOSED_REMOTE, PRE_CLOSED, CLOSED
  }

  private volatile State state = State.OPEN;

  public Http2StreamChannel(Channel parent, Http2Stream stream) {
    super(parent);
    this.http2ConnHandlerCtx =
        parent.pipeline().context(Http2ConnectionHandler.class);
    Http2ConnectionHandler connHandler =
        (Http2ConnectionHandler) http2ConnHandlerCtx.handler();
    this.stream = stream;
    this.localFlowController =
        connHandler.connection().local().flowController();
    this.encoder = connHandler.encoder();
    this.config = new DefaultChannelConfig(this);
  }

  public Http2Stream stream() {
    return stream;
  }

  @Override
  public ChannelConfig config() {
    return config;
  }

  @Override
  public boolean isOpen() {
    return state != State.CLOSED;
  }

  @Override
  public boolean isActive() {
    return isOpen();
  }

  @Override
  public ChannelMetadata metadata() {
    return METADATA;
  }

  private final class Http2Unsafe extends AbstractUnsafe {

    @Override
    public void connect(SocketAddress remoteAddress,
        SocketAddress localAddress, ChannelPromise promise) {
      throw new UnsupportedOperationException();
    }

    public void forceFlush() {
      super.flush0();
    }
  }

  @Override
  protected AbstractUnsafe newUnsafe() {
    return new Http2Unsafe();
  }

  @Override
  protected boolean isCompatible(EventLoop loop) {
    return true;
  }

  @Override
  protected SocketAddress localAddress0() {
    return parent().localAddress();
  }

  @Override
  protected SocketAddress remoteAddress0() {
    return parent().remoteAddress();
  }

  @Override
  protected void doBind(SocketAddress localAddress) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void doDisconnect() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void doClose() throws Exception {
    for (InboundMessage msg; (msg = inboundMessageQueue.poll()) != null;) {
      ReferenceCountUtil.release(msg.msg);
      localFlowController.consumeBytes(stream, msg.length);
    }
    if (state != State.PRE_CLOSED) {
      encoder.writeRstStream(http2ConnHandlerCtx, stream.id(),
        Http2Error.INTERNAL_ERROR.code(), http2ConnHandlerCtx.newPromise());
    }
    state = State.CLOSED;
  }

  private final Runnable readTask = new Runnable() {
    @Override
    public void run() {
      ChannelPipeline pipeline = pipeline();
      for (InboundMessage m; (m = inboundMessageQueue.poll()) != null;) {
        if (m.msg == LastHttp2Message.get()) {
          state =
              state == State.HALF_CLOSED_LOCAL ? State.PRE_CLOSED
                  : State.HALF_CLOSED_REMOTE;
        }
        try {
          if (m.length > 0
              && localFlowController.consumeBytes(stream, m.length)) {
            http2ConnHandlerCtx.channel().flush();
          }
        } catch (Http2Exception e) {
          // an Http2Exception at least means the stream is broken(maybe the
          // whole connection), so we are out.
          http2ConnHandlerCtx.pipeline().fireExceptionCaught(e);
          return;
        }
        pipeline.fireChannelRead(m.msg);
      }
      pipeline.fireChannelReadComplete();
      if (state == State.PRE_CLOSED) {
        close();
      }
    }
  };

  @Override
  protected void doBeginRead() throws Exception {
    if (inboundMessageQueue.isEmpty()) {
      return;
    }
    State currentState = this.state;
    if (remoteSideClosed(currentState)) {
      throw new ClosedChannelException();
    }
    final InternalThreadLocalMap threadLocals = InternalThreadLocalMap.get();
    final Integer stackDepth = threadLocals.localChannelReaderStackDepth();
    if (stackDepth < MAX_READER_STACK_DEPTH) {
      threadLocals.setLocalChannelReaderStackDepth(stackDepth + 1);
      try {
        readTask.run();
      } finally {
        threadLocals.setLocalChannelReaderStackDepth(stackDepth);
      }
    } else {
      eventLoop().execute(readTask);
    }
  }

  private void resumeWrite() {
    writePending = false;
    ((Http2Unsafe) unsafe()).forceFlush();
  }

  @Override
  protected void doWrite(ChannelOutboundBuffer in) throws Exception {
    State currentState = this.state;
    if (localSideClosed(currentState)) {
      throw new ClosedChannelException();
    }
    int writeBufferHighWaterMark = config().getWriteBufferHighWaterMark();

    boolean flush = false;
    for (;;) {
      if (pendingOutboundBytes >= writeBufferHighWaterMark) {
        writePending = true;
        break;
      }
      Object msg = in.current();
      if (msg == null) {
        break;
      }
      if (msg == LastHttp2Message.get()) {
        this.state =
            currentState == State.HALF_CLOSED_REMOTE ? State.PRE_CLOSED
                : State.HALF_CLOSED_LOCAL;
        encoder.writeData(http2ConnHandlerCtx, stream.id(),
          Unpooled.EMPTY_BUFFER, 0, true, http2ConnHandlerCtx.newPromise());
      } else if (msg instanceof Http2Headers) {
        encoder.writeHeaders(http2ConnHandlerCtx, stream.id(),
          (Http2Headers) msg, 0, false, http2ConnHandlerCtx.newPromise());
      } else if (msg instanceof ByteBuf) {
        ByteBuf data = (ByteBuf) msg;
        final int pendingBytes = data.readableBytes();
        pendingOutboundBytes += pendingBytes;
        encoder.writeData(http2ConnHandlerCtx, stream.id(), data.retain(), 0,
          false, http2ConnHandlerCtx.newPromise()).addListener(
          new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future)
                throws Exception {
              pendingOutboundBytes -= pendingBytes;
              if (writePending
                  && pendingOutboundBytes <= config()
                      .getWriteBufferLowWaterMark()) {
                resumeWrite();
              }
            }
          });
      } else {
        throw new UnsupportedMessageTypeException(msg, Http2Headers.class,
            ByteBuf.class);
      }
      in.remove();
      flush = true;
    }
    if (flush) {
      http2ConnHandlerCtx.channel().flush();
    }
    if (state == State.PRE_CLOSED) {
      close();
    }
  }

  public void writeInbound(Object msg, int length) {
    inboundMessageQueue.add(new InboundMessage(msg, length));
  }

  private static final ImmutableSet<State> REMOTE_SIDE_CLOSED_STATES =
      ImmutableSet.of(State.HALF_CLOSED_REMOTE, State.PRE_CLOSED, State.CLOSED);

  public boolean remoteSideClosed() {
    return remoteSideClosed(state);
  }

  private boolean remoteSideClosed(State state) {
    return REMOTE_SIDE_CLOSED_STATES.contains(state);
  }

  private static final ImmutableSet<State> LOCAL_SIDE_CLOSED_STATES =
      ImmutableSet.of(State.HALF_CLOSED_LOCAL, State.PRE_CLOSED, State.CLOSED);

  public boolean localSideClosed() {
    return localSideClosed(state);
  }

  private boolean localSideClosed(State state) {
    return LOCAL_SIDE_CLOSED_STATES.contains(state);
  }

  public void setClosed() {
    State currentState = this.state;
    if (!remoteSideClosed(currentState)) {
      writeInbound(LastHttp2Message.get(), 0);
      if (config().isAutoRead()) {
        read();
      }
    }
    currentState = this.state;
    if (!localSideClosed(currentState)) {
      this.state =
          currentState == State.HALF_CLOSED_REMOTE ? State.PRE_CLOSED
              : State.HALF_CLOSED_LOCAL;
      if (currentState == State.PRE_CLOSED) {
        close();
      }
    }
  }
}
