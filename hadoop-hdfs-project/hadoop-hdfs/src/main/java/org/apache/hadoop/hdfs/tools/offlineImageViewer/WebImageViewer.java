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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

import com.google.common.annotations.VisibleForTesting;

/**
 * WebImageViewer loads a fsimage and exposes read-only WebHDFS API for its
 * namespace.
 */
public class WebImageViewer {
  public static final Log LOG = LogFactory.getLog(WebImageViewer.class);

  private Channel channel;
  private InetSocketAddress address;
  private final ChannelFactory factory =
      new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool(), 1);
  private final ServerBootstrap bootstrap = new ServerBootstrap(factory);

  static final ChannelGroup allChannels =
      new DefaultChannelGroup("WebImageViewer");

  public WebImageViewer(InetSocketAddress address) {
    this.address = address;
  }

  /**
   * Start WebImageViewer and wait until the thread is interrupted.
   * @param fsimage the fsimage to load.
   * @throws IOException if failed to load the fsimage.
   */
  public void initServerAndWait(String fsimage) throws IOException {
    initServer(fsimage);
    try {
      channel.getCloseFuture().await();
    } catch (InterruptedException e) {
      LOG.info("Interrupted. Stopping the WebImageViewer.");
      shutdown();
    }
  }

  /**
   * Start WebImageViewer.
   * @param fsimage the fsimage to load.
   * @throws IOException if fail to load the fsimage.
   */
  @VisibleForTesting
  public void initServer(String fsimage) throws IOException {
    FSImageLoader loader = FSImageLoader.load(fsimage);

    ChannelPipeline pipeline = Channels.pipeline();
    pipeline.addLast("channelTracker", new SimpleChannelUpstreamHandler() {
      @Override
      public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
          throws Exception {
        allChannels.add(e.getChannel());
      }
    });
    pipeline.addLast("httpDecoder", new HttpRequestDecoder());
    pipeline.addLast("requestHandler", new FSImageHandler(loader));
    pipeline.addLast("stringEncoder", new StringEncoder());
    pipeline.addLast("httpEncoder", new HttpResponseEncoder());
    bootstrap.setPipeline(pipeline);
    channel = bootstrap.bind(address);
    allChannels.add(channel);

    address = (InetSocketAddress) channel.getLocalAddress();
    LOG.info("WebImageViewer started. Listening on " + address.toString()
        + ". Press Ctrl+C to stop the viewer.");
  }

  /**
   * Stop WebImageViewer.
   */
  @VisibleForTesting
  public void shutdown() {
    allChannels.close().awaitUninterruptibly();
    factory.releaseExternalResources();
  }

  /**
   * Get the listening port.
   * @return the port WebImageViewer is listening on
   */
  @VisibleForTesting
  public int getPort() {
    return address.getPort();
  }
}
