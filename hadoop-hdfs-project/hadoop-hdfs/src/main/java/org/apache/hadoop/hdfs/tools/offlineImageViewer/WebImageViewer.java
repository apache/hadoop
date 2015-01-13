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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * WebImageViewer loads a fsimage and exposes read-only WebHDFS API for its
 * namespace.
 */
public class WebImageViewer implements Closeable {
  public static final Log LOG = LogFactory.getLog(WebImageViewer.class);

  private Channel channel;
  private InetSocketAddress address;

  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ChannelGroup allChannels;

  public WebImageViewer(InetSocketAddress address) {
    this.address = address;
    this.bossGroup = new NioEventLoopGroup();
    this.workerGroup = new NioEventLoopGroup();
    this.allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    this.bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NioServerSocketChannel.class);
  }

  /**
   * Start WebImageViewer and wait until the thread is interrupted.
   * @param fsimage the fsimage to load.
   * @throws IOException if failed to load the fsimage.
   */
  public void start(String fsimage) throws IOException {
    try {
      initServer(fsimage);
      channel.closeFuture().await();
    } catch (InterruptedException e) {
      LOG.info("Interrupted. Stopping the WebImageViewer.");
      close();
    }
  }

  /**
   * Start WebImageViewer.
   * @param fsimage the fsimage to load.
   * @throws IOException if fail to load the fsimage.
   */
  @VisibleForTesting
  public void initServer(String fsimage)
          throws IOException, InterruptedException {
    final FSImageLoader loader = FSImageLoader.load(fsimage);

    bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new HttpRequestDecoder(),
          new StringEncoder(),
          new HttpResponseEncoder(),
          new FSImageHandler(loader, allChannels));
      }
    });

    channel = bootstrap.bind(address).sync().channel();
    allChannels.add(channel);

    address = (InetSocketAddress) channel.localAddress();
    LOG.info("WebImageViewer started. Listening on " + address.toString() + ". Press Ctrl+C to stop the viewer.");
  }

  /**
   * Get the listening port.
   * @return the port WebImageViewer is listening on
   */
  @VisibleForTesting
  public int getPort() {
    return address.getPort();
  }

  @Override
  public void close() {
    allChannels.close().awaitUninterruptibly();
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }
}
