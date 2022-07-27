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

package org.apache.hadoop.ipc.netty.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigurationWithLogging;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.Time;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;

class NettyIpcStreams extends IpcStreams {
  private final EventLoopGroup group;
  private io.netty.channel.Channel channel;
  private int soTimeout;
  private IOException channelIOE;

  public NettyIpcStreams(Socket socket) throws IOException {
    soTimeout = socket.getSoTimeout();
    //TODO: Resource Leak Detection for Netty is turned off for now. Decision on
    //      selective turning on, will be taken later. This code will be
    //      changed then. The ResourceLeakDetector level can also be made
    //      configurable.
    if (!LOG.isDebugEnabled()) {
      ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
    }
    channel = new NioSocketChannel(socket.getChannel());

    // TODO: If you have not set autoread to false you may get into trouble if
    //  one channel writes a lot of data before the other can consume it. As
    //  it is all asynchronous one may end up with buffers that have too much
    //  data and encounter a Out of Memory Exception. Revisit at a later time to
    //  check if it will improve the performance of reads.
    channel.config().setAutoRead(false);

    SslHandler sslHandler = null;

    if (IpcStreams.useSSLSelfSignedCertificate) {
      SslContext sslCtx = null;

      try {
        sslCtx = SslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
      } catch (SSLException e) {
        throw new IOException("Exception while building SSL Context", e);
      }

      sslHandler = sslCtx.newHandler(channel.alloc());
    }
    else {
      Configuration sslConf = new ConfigurationWithLogging(
          SSLFactory.readSSLConfiguration(conf, SSLFactory.Mode.CLIENT));
      SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, sslConf);

      try {
        sslFactory.init();
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }

      try {
        SSLEngine clientSSLEngine =  sslFactory.createSSLEngine();
        sslHandler = new SslHandler(sslFactory.createSSLEngine());
        sslHandler.handshakeFuture().addListener(
            new GenericFutureListener<Future<Channel>>() {
              @Override
              public void operationComplete(
                  final Future<Channel> handshakeFuture)
                  throws Exception {
                if (handshakeFuture.isSuccess()) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("TLS handshake success");
                  }
                } else {
                  throw new IOException(
                      "TLS handshake failed." + handshakeFuture.cause());
                }
              }
            });
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
    }

    channel.pipeline().addLast("SSL", sslHandler);
    RpcChannelHandler handler = new RpcChannelHandler();
    setInputStream(new BufferedInputStream(handler.getInputStream()));
    setOutputStream(new BufferedOutputStream(handler.getOutputStream()));
    channel.pipeline().addLast("RPC", handler);
    group = new NioEventLoopGroup(1);
    group.register(channel);

    try {
      channel.pipeline().get(SslHandler.class).handshakeFuture().sync();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for SSL Handshake", e);
    }
  }

  @Override
  public java.util.concurrent.Future<?> submit(Runnable runnable) {
    return Client.getClientExecutor().submit(runnable);
  }

  @Override
  public void close() {
    if (channel.isRegistered()) {
      channel.writeAndFlush(Unpooled.EMPTY_BUFFER)
          .addListener(ChannelFutureListener.CLOSE);
    }
  }

  private class NettyInputStream extends InputStream {
    private final CompositeByteBuf cbuf;

    NettyInputStream(CompositeByteBuf cbuf) {
      this.cbuf = cbuf;
    }

    @Override
    public int read() throws IOException {
      // buffered stream ensures this isn't called.
      throw new UnsupportedOperationException();
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
      synchronized (cbuf) {
        // trigger a read if the channel isn't at EOF but has less
        // buffered bytes than requested.  the method may still return
        // less bytes than requested.
        int readable = readableBytes();
        if (readable != -1 && readable < len) {
          long until = Time.monotonicNow() + soTimeout;
          long timeout = soTimeout;
          channel.read();
          readable = readableBytes();

          // Reads in Netty are asynchronous. channel.read() can actually
          // return without having read the data. Hence, wait until the
          // read returns bytes to be read. It is possible that the read
          // did not return enough bytes. This just means the method returns
          // less bytes than expected.
          while (readable == 0 && timeout > 0) {
            try {
              cbuf.wait(timeout);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new InterruptedIOException(e.getMessage());
            }
            readable = readableBytes();
            timeout = until - Time.monotonicNow();
          }
        }

        switch(readable) {
          case -1: // The channel closed before the read could be finished.
            return -1;
          case 0: // Even after waiting the channel read did not complete.
            throw new SocketTimeoutException(
                soTimeout + " millis timeout while " +
                    "waiting for channel to be ready for read. ch : "
                    + channel + " peer address : " + channel.remoteAddress());
          default:
            // return as many bytes as possible.
            len = Math.min(len, readable);
            cbuf.readBytes(b, off, len).discardReadComponents();
            return len;
        }
      }
    }

    // if the buffer is empty and an error has occurred, throw it.
    // else return the number of buffered bytes, 0 if empty and the
    // connection is open, -1 upon EOF.
    int readableBytes() throws IOException {
      int readable = cbuf.readableBytes();
      if (readable == 0 && channelIOE != null) {
        throw channelIOE;
      }

      return (readable > 0) ? readable : channel.isActive() ? 0 : -1;
    }
  }

  private class NettyOutputStream extends OutputStream {
    private final CompositeByteBuf cbuf;

    NettyOutputStream(CompositeByteBuf cbuf) {
      this.cbuf = cbuf;
    }

    @Override
    public void write(int b) throws IOException {
      // buffered stream ensures this isn't called.
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte b[], int off, int len) {
      ByteBuf buf = Unpooled.wrappedBuffer(b, off, len);
      channel.write(buf);
    }

    @Override
    public void flush() {
      channel.writeAndFlush(Unpooled.EMPTY_BUFFER).
          awaitUninterruptibly(soTimeout);
    }
  }

  private class RpcChannelHandler extends ChannelInboundHandlerAdapter {
    // aggregates unread buffers.  should be no more than 2 at a time.
    private final CompositeByteBuf cbuf =
        ByteBufAllocator.DEFAULT.compositeBuffer();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
      if (msg instanceof ByteBuf) {
        synchronized (cbuf) {
          cbuf.addComponent(true, (ByteBuf) msg);
          cbuf.notifyAll();
        }
      } else {
        super.channelRead(ctx, msg);
      }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      // channel is unregistered when completely closed.  each channel
      // has a dedicated event loop so schedule it for shutdown and
      // release its buffer.
      if (channel.isOpen()) {
        channel.writeAndFlush(Unpooled.EMPTY_BUFFER)
            .addListener(ChannelFutureListener.CLOSE);
      }
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      synchronized (cbuf) {
        cbuf.release();
        cbuf.notifyAll();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,
                                Throwable cause) {
      // probably connection reset by peer.
      synchronized (cbuf) {
        channelIOE = toException(cause);
        cbuf.notifyAll();
      }
    }

    InputStream getInputStream() {
      return new NettyInputStream(cbuf);
    }

    OutputStream getOutputStream() {
      return new NettyOutputStream(cbuf);
    }

    IOException timeout(String op) {
      return new SocketTimeoutException(
          soTimeout + " millis timeout while " +
              "waiting for channel to be ready for " +
              op + ". ch : " + channel);
    }
  }

  private static IOException toException(Throwable t) {
    if (t.getClass().getPackage().getName().startsWith("io.netty")) {
      String[] parts = t.getMessage().split(" failed: ");
      String shortMessage = parts[parts.length - 1];
      if (t instanceof io.netty.channel.ConnectTimeoutException) {
        return new ConnectTimeoutException(shortMessage);
      }
      if (t instanceof ConnectException) {
        return new ConnectException(shortMessage);
      }
      return new SocketException(shortMessage);
    } else if (t instanceof IOException) {
      return (IOException) t;
    }
    return new IOException(t);
  }
}
