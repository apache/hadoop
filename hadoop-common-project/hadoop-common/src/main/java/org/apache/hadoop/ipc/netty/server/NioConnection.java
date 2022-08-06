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

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

public class NioConnection extends Connection<SocketChannel> {
  private final Server server;
  private final Socket socket;
  public final LinkedList<Server.RpcCall> responseQueue = new LinkedList<>();

  public NioConnection(Server server, SocketChannel channel) throws
      IOException {
    super(server, channel, (InetSocketAddress) channel.getLocalAddress(),
        (InetSocketAddress) channel.getRemoteAddress());
    this.server = server;
    socket = channel.socket();
  }

  @Override
  public boolean isOpen() {
    return channel.isOpen();
  }

  @Override
  protected void setSendBufferSize(SocketChannel channel, int size)
      throws SocketException {
    channel.socket().setSendBufferSize(size);
  }

  @Override
  protected int bufferRead(Object channel, ByteBuffer buf) throws IOException {
    return server.channelRead((ReadableByteChannel) channel, buf);
  }

  @Override
  synchronized public void close() {
    super.close();
    if (!channel.isOpen()) {
      return;
    }
    try {
      socket.shutdownInput(); // prevent connection reset on client.
      socket.shutdownOutput();
    } catch (Exception e) {
      Server.LOG.debug("Ignoring socket shutdown exception", e);
    }
    if (channel.isOpen()) {
      IOUtils.cleanupWithLogger(Server.LOG, channel);
    }
    IOUtils.cleanupWithLogger(Server.LOG, socket);
  }
}
