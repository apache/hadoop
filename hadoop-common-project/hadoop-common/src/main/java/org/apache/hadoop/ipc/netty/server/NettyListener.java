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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.bootstrap.ServerBootstrapConfig;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class NettyListener implements Listener<Channel> {
  private final Server server;
  private final ServerBootstrap bootstrap;
  private final NettyThreadFactory listenerFactory;
  private final NettyThreadFactory readerFactory;
  private final ChannelGroup acceptChannels =
      new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final int backlogLength;
  private final InetSocketAddress address;
  private final Channel channel;

  public NettyListener(Server server, int port) throws IOException {
    this.server = server;
    if (!Server.LOG.isDebugEnabled()) {
      ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
    }
    backlogLength = server.conf.getInt(
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
    Class<? extends io.netty.channel.socket.ServerSocketChannel> channelClass;
    listenerFactory = new NettyThreadFactory(server, "Netty Socket Acceptor", port);
    readerFactory = new NettyThreadFactory(server,"Netty Socket Reader", port);

    // netty's readers double as responders so double the readers to
    // compensate.
    int numReaders = 2 * server.getNumReaders();
    EventLoopGroup acceptors;
    EventLoopGroup readers;
    // Attempt to use native transport if available.
    if (Epoll.isAvailable()) { // Linux.
      channelClass = EpollServerSocketChannel.class;
      acceptors = new EpollEventLoopGroup(1, listenerFactory);
      readers = new EpollEventLoopGroup(numReaders, readerFactory);
    } else if (KQueue.isAvailable()) { // OS X/BSD.
      channelClass = KQueueServerSocketChannel.class;
      acceptors = new KQueueEventLoopGroup(1, listenerFactory);
      readers = new KQueueEventLoopGroup(numReaders, readerFactory);
    } else {
      channelClass = NioServerSocketChannel.class;
      acceptors = new NioEventLoopGroup(1, listenerFactory);
      readers = new NioEventLoopGroup(numReaders, readerFactory);
    }
    bootstrap = new ServerBootstrap()
        .group(acceptors, readers)
        .channel(channelClass)
        .option(ChannelOption.SO_BACKLOG, backlogLength)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childOption(ChannelOption.RCVBUF_ALLOCATOR,
            Server.IPC_RECVBUF_ALLOCATOR)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, server.tcpNoDelay)
        .childHandler(new ChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel channel)
              throws IOException {
            server.connectionManager.register(
                new NettyConnection(server, channel));
          }
        });

    address = new InetSocketAddress(server.bindAddress, port);
    channel = Server.bind(Binder.NETTY, bootstrap,
        address, backlogLength, server.conf, server.portRangeConfig);
    registerAcceptChannel(channel);
    // If may have been an ephemeral port or port range bind, so update
    // the thread factories to rename any already created threads.
    port = ((InetSocketAddress) channel.localAddress()).getPort();
    listenerFactory.updatePort(port);
    readerFactory.updatePort(port);
  }

  @Override
  public InetSocketAddress getAddress() {
    return (InetSocketAddress) channel.localAddress();
  }

  @Override
  public void listen(InetSocketAddress addr) throws IOException {
    registerAcceptChannel(
        Binder.NETTY.bind(bootstrap, addr, backlogLength));
  }

  @Override
  public void registerAcceptChannel(Channel channel) {
    acceptChannels.add(channel);
  }

  @Override
  public void closeAcceptChannels() {
    acceptChannels.close();
  }

  @Override
  public void start() {
    server.connectionManager.startIdleScan();
  }

  @Override
  public void interrupt() {
  }

  @Override
  public void doStop() {
    try {
      // closing will send events to the bootstrap's event loop groups.
      closeAcceptChannels();
      server.connectionManager.stopIdleScan();
      server.connectionManager.closeAll();
      // shutdown the event loops to reject all further events.
      ServerBootstrapConfig config = bootstrap.config();
      config.group().shutdownGracefully(0, 1, TimeUnit.SECONDS);
      config.childGroup().shutdownGracefully(0, 1, TimeUnit.SECONDS);
      // wait for outstanding close events to be processed.
      config.group().terminationFuture().awaitUninterruptibly();
      config.childGroup().terminationFuture().awaitUninterruptibly();
    } finally {
      IOUtils.cleanupWithLogger(Server.LOG, listenerFactory, readerFactory);
    }
  }
}
