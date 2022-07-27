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

import org.apache.hadoop.ipc.Server;

import java.io.IOException;
import java.net.InetSocketAddress;

/*
 * Common interface for the listener, implemented for both the NIO and the
 * NETTY listener classes.
 */
public interface Listener<T> {
  /**
   * Create a new instance of the Listener implementation depending on
   * whether the listener is being created for native Java NIO sockets
   * or for Netty Channels.
   *
   * @param server The server object for which the socket abstraction are
   *               being created.
   * @param port   The port on which the server will listen for connections.
   * @return An instance of the Listener interface implementation.
   * @throws IOException If an exception occurs while creating the listener
   *                     object.
   */
  static Listener newInstance(Server server, int port) throws IOException {
    return server.useNetty()
        ? new NettyListener(server, port)
        : new NioListener(server, port);
  }

  /**
   * Bind the server socket channel to the local host and port.
   *
   * @param addr The InetSocketAddress class that encapsulates the IP socket
   *             address to bind to.
   * @throws IOException Throws an exception if there is a problem while
   *                     listening.
   */
  void listen(InetSocketAddress addr) throws IOException;

  /**
   * Register the channel to the list of channels we are listening on.
   *
   * @param channel The channel that needs to be registered.
   * @throws IOException Throws an exception if there is a problem while
   *                     registering the channel.
   */
  void registerAcceptChannel(T channel) throws IOException;

  /**
   * Close all the accepted channels.
   *
   * @throws IOException Throws an exception if there is a problem closing any
   *                     of the channels.
   */
  void closeAcceptChannels() throws IOException;

  /**
   * Return the local socket address associated with the socket.
   *
   * @return The InetSocketAddress class that encapsulates the IP socket
   * address to bind to.
   */
  InetSocketAddress getAddress();

  /**
   * Start the idle scanner that checks for connections that have been
   * inactive beyond a configured threshold.
   */
  void start();

  void interrupt();

  /**
   * Close all Channels and Readers.
   */
  void doStop();
}
