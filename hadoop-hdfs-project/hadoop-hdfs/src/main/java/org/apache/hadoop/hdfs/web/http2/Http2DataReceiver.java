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
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A helper class that wrapper the HTTP/2 data frame as an {@link InputStream}.
 * <p>
 * Notice that, this classes can be used together with
 * {@link ReadTimeoutHandler} to limit the waiting time when reading data.
 */
@InterfaceAudience.Private
public class Http2DataReceiver extends ChannelInboundHandlerAdapter {

  private static final Component END_OF_STREAM = new Component(null, 0);

  private static final EOFException EOF = new EOFException();

  private static final class Component {

    public final ByteBuf buf;

    public final int length;

    public Component(ByteBuf buf) {
      this(buf, buf.readableBytes());
    }

    public Component(ByteBuf buf, int length) {
      this.buf = buf;
      this.length = length;
    }

  }

  private final Deque<Component> queue = new ArrayDeque<Component>();

  private int queuedBytes;

  private Channel channel;

  private Throwable error;

  private Http2Headers headers;

  private final ByteBufferReadableInputStream contentInput =
      new ByteBufferReadableInputStream() {

        @Override
        public int read() throws IOException {
          Component comp = peekUntilAvailable();
          if (comp == END_OF_STREAM) {
            return -1;
          }
          int b = comp.buf.readByte() & 0xFF;
          if (!comp.buf.isReadable()) {
            removeHead();
          }
          return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          Component comp = peekUntilAvailable();
          if (comp == END_OF_STREAM) {
            return -1;
          }
          int bufReadableBytes = comp.buf.readableBytes();
          if (len >= bufReadableBytes) {
            comp.buf.readBytes(b, off, bufReadableBytes);
            removeHead();
            return bufReadableBytes;
          } else {
            comp.buf.readBytes(b, off, len);
            return len;
          }
        }

        @Override
        public long skip(long n) throws IOException {
          Component comp = peekUntilAvailable();
          if (comp == END_OF_STREAM) {
            return 0L;
          }
          int bufReadableBytes = comp.buf.readableBytes();
          if (n >= bufReadableBytes) {
            removeHead();
            return bufReadableBytes;
          } else {
            comp.buf.skipBytes((int) n);
            return n;
          }
        }

        @Override
        public int read(ByteBuffer bb) throws IOException {
          Component comp = peekUntilAvailable();
          if (comp == END_OF_STREAM) {
            return -1;
          }
          int bbRemaining = bb.remaining();
          int bufReadableBytes = comp.buf.readableBytes();
          if (bbRemaining >= bufReadableBytes) {
            int toRestoredLimit = bb.limit();
            bb.limit(bb.position() + bufReadableBytes);
            comp.buf.readBytes(bb);
            bb.limit(toRestoredLimit);
            removeHead();
            return bufReadableBytes;
          } else {
            comp.buf.readBytes(bb);
            return bbRemaining;
          }
        }

        private boolean closed = false;

        @Override
        public void close() throws IOException {
          if (closed) {
            return;
          }
          synchronized (queue) {
            if (error == null) {
              error = EOF;
            }
          }
          channel.close().addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future)
                throws Exception {
              synchronized (queue) {
                for (Component c; (c = queue.peek()) != null;) {
                  if (c == END_OF_STREAM) {
                    return;
                  }
                  c.buf.release();
                  queue.remove();
                }
              }
            }
          });
        }

      };

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
      throws Exception {
    if (msg == LastHttp2Message.get()) {
      enqueue(END_OF_STREAM);
    } else if (msg instanceof Http2Headers) {
      synchronized (queue) {
        headers = (Http2Headers) msg;
        queue.notifyAll();
      }
    } else if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      if (buf.isReadable()) {
        enqueue(new Component(buf));
      } else {
        buf.release();
      }
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    synchronized (queue) {
      if (error == null) {
        error = cause;
        queue.notifyAll();
      }
    }
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    channel = ctx.channel();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    synchronized (queue) {
      if (error != null) {
        return;
      }
      Component lastComp = queue.peekLast();
      if (lastComp == END_OF_STREAM) {
        return;
      }
      error = EOF;
      notifyAll();
    }
  }

  private void enqueue(Component comp) {
    synchronized (queue) {
      queuedBytes += comp.length;
      if (queuedBytes >= channel.config().getWriteBufferHighWaterMark()) {
        channel.config().setAutoRead(false);
      }
      queue.add(comp);
      queue.notifyAll();
    }
  }

  private Component peekUntilAvailable() throws IOException {
    Throwable cause;
    synchronized (queue) {
      for (;;) {
        if (!queue.isEmpty()) {
          return queue.peek();
        }
        if (error != null) {
          cause = error;
          break;
        }
        try {
          queue.wait();
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        }
      }
    }
    propagate(cause);
    return null;
  }

  private void removeHead() {
    Component comp;
    synchronized (queue) {
      comp = queue.remove();
      queuedBytes -= comp.length;
      ChannelConfig config = channel.config();
      if (!config.isAutoRead()
          && queuedBytes < config.getWriteBufferLowWaterMark()) {
        config.setAutoRead(true);
      }
    }
    comp.buf.release();
  }

  private void propagate(Throwable cause) throws IOException {
    if (cause == ReadTimeoutException.INSTANCE) {
      throw new IOException("Read timeout");
    } else if (cause == EOF) {
      throw new IOException("Stream reset by peer: " + channel.remoteAddress());
    } else if (cause instanceof IOException) {
      throw (IOException) cause;
    } else if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    } else {
      throw new IOException(cause);
    }
  }

  public Http2Headers waitForResponse() throws IOException {
    Throwable cause;
    synchronized (queue) {
      for (;;) {
        if (error != null) {
          cause = error;
          break;
        }
        if (headers != null) {
          return headers;
        }
        try {
          queue.wait();
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        }
      }
    }
    propagate(cause);
    return null;
  }

  /**
   * The returned stream is not thread safe.
   */
  public ByteBufferReadableInputStream content() {
    return contentInput;
  }

}