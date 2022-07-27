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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * The interface encapsulates the functionality required to bind to an endpoint.
 * The underlying abstractions are different for native java sockets and
 * for NETTY. Hence, we present two different objects of anonymous classes, each
 * for one type of endpoint.
 *
 * @param <T> Input endpoint abstraction.
 * @param <O> Output endpoint abstraction after the bind succeeded.
 */
public interface Binder<T, O> {
  /**
   * Bind the socket channel to a local host and port. The bind method
   * implementation that varies according to whether it operates on a native
   * java socket or a netty channel.
   *
   * @param obj     A ServerSocket or a ServerBootstrap object.
   * @param addr    The InetSocketAddress class that encapsulates the IP
   *                socket address to bind to.
   * @param backlog Pending connections on the socket.
   * @return Bootstrap object.
   * @throws IOException If a problem happens during the bind.
   */
  T bind(O obj, InetSocketAddress addr, int backlog)
      throws IOException;

  Binder<ServerSocket, ServerSocket> NIO =
      new Binder<ServerSocket, ServerSocket>() {
        /**
         * Bind the input socket to the input address.
         *
         * @param socket  Input ServerSocket.
         * @param addr    The InetSocketAddress class that encapsulates the IP
         *                socket address to bind to.
         * @param backlog Pending connections on the socket.
         * @return A ServerSocket object that is bound to the input address.
         * @throws IOException If a problem happens during the bind.
         */
        @Override
        public ServerSocket bind(ServerSocket socket,
                                 InetSocketAddress addr,
                                 int backlog) throws IOException {
          if (socket.isBound()) {
            throw new BindException("Address already in use");
          }
          socket.bind(addr, backlog);
          return socket;
        }
      };

  // Bootstrap a Netty Channel to the input port.
  Binder<Channel, ServerBootstrap> NETTY =
      new Binder<Channel, ServerBootstrap>() {
        /**
         * Bind the ServerBootstrap object to the input address.
         *
         * @param bootstrap Input ServerBootstrap class that encapsulates the
         *                  Netty abstractions that bootstrap a server channel.
         * @param addr      The InetSocketAddress class that encapsulates the IP
         *                  socket address to bind to.
         * @param backlog   Pending connections on the socket.
         * @return A Channel that is bootstrapped to the input address.
         * @throws IOException If a problem happens during the bind.
         */
        @Override
        public Channel bind(ServerBootstrap bootstrap,
                            InetSocketAddress addr,
                            int backlog) throws IOException {
          bootstrap.option(ChannelOption.SO_BACKLOG, backlog);
          // netty throws private undeclared checked exceptions from the
          // future.  it's ugly, but mincing the message is necessary.
          ChannelFuture future = bootstrap.bind(addr).awaitUninterruptibly();
          if (!future.isSuccess()) {
            Throwable t = future.cause();
            if (t.getMessage().contains("Address already in use")) {
              throw new BindException(t.getMessage());
            }
          }
          return future.channel(); // will throw any other exceptions.
        }
      };
}
