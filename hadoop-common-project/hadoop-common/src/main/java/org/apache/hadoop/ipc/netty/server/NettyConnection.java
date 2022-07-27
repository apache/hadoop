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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.ssl.SSLFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.util.List;

public class NettyConnection extends Connection<Channel> {
  private final Server server;

  public NettyConnection(Server server,
                         Channel channel)
      throws IOException {
    super(server, channel, (InetSocketAddress) channel.localAddress(),
        (InetSocketAddress) channel.remoteAddress());
    this.server = server;
    ChannelInboundHandler decoder = new ByteToMessageDecoder() {
      @Override
      public void decode(ChannelHandlerContext ctx, ByteBuf in,
                         List<Object> out) throws Exception {
        doRead(in);
      }

      // client closed the connection.
      @Override
      public void channelInactive(ChannelHandlerContext ctx) {
        server.connectionManager.close(NettyConnection.this);
      }
    };

    SslHandler sslHandler = null;

    boolean useSSLSelfSignedCertificate = server.conf.getBoolean(
        CommonConfigurationKeys.IPC_SSL_SELF_SIGNED_CERTIFICATE_TEST,
        CommonConfigurationKeys.IPC_SSL_SELF_SIGNED_CERTIFICATE_TEST_DEFAULT);

    if (useSSLSelfSignedCertificate) {
      Server.LOG.warn(
          "The use of the netty self-signed certificate is insecure." +
              " It is currently used for ease of unit testing in the code" +
              " and is liable to be removed in later versions.");
      SelfSignedCertificate ssc = null;

      try {
        ssc = new SelfSignedCertificate();
      } catch (CertificateException e) {
        throw new IOException(
            "Exception while creating a SelfSignedCertificate object.", e);
      }

      SslContext sslCtx = null;

      try {
        sslCtx =
            SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
                .build();
      } catch (SSLException e) {
        throw new IOException("Exception while building a SSLContext", e);
      }

      sslHandler = sslCtx.newHandler(channel.alloc());
    } else {
      SSLFactory sslFactory =
          new SSLFactory(SSLFactory.Mode.SERVER, server.conf);

      try {
        sslFactory.init();
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }

      try {
        sslHandler = new SslHandler(sslFactory.createSSLEngine());
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
    }

    if (sslHandler != null) {
      sslHandler.handshakeFuture()
          .addListener(new GenericFutureListener<Future<Channel>>() {
            @Override
            public void operationComplete(final Future<Channel> handshakeFuture)
                throws Exception {
              if (handshakeFuture.isSuccess()) {
                if (Server.LOG.isDebugEnabled()) {
                  Server.LOG.debug("TLS handshake success");
                }
              } else {
                throw new IOException(
                    "TLS handshake failed." + handshakeFuture.cause());
              }
            }
          });
    }

    if (Server.LOG.isDebugEnabled()) {
      Server.LOG.debug("Adding the SSLHandler to the pipeline");
    }

    channel.pipeline().addLast("SSL", sslHandler);
    // decoder maintains state, responder doesn't so it can be reused.
    channel.pipeline().addLast("RPC", new CombinedChannelDuplexHandler(
        decoder, (NettyResponder) server.responder));
  }

  @Override
  protected void setSendBufferSize(Channel channel, int size) {
    channel.config().setOption(ChannelOption.SO_SNDBUF, size);
  }

  @Override
  public boolean isOpen() {
    return channel.isOpen();
  }

  @Override
  public boolean setShouldClose() {
    channel.config().setAutoRead(false); // stop reading more requests.
    return super.setShouldClose();
  }

  @Override
  synchronized public void close() {
    channel.writeAndFlush(Unpooled.EMPTY_BUFFER)
        .addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public int bufferRead(Object in, ByteBuffer buf) {
    ByteBuf inBuf = (ByteBuf) in;
    int length = buf.remaining();
    if (inBuf.readableBytes() < length) {
      return 0;
    }
    inBuf.readBytes(buf);
    server.rpcMetrics.incrReceivedBytes(length);
    return length;
  }
}
