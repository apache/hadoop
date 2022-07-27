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

/**
 * Common interface for native Java NIO and Java Netty classes that respond
 * to incoming RPC calls.
 */
public interface Responder {
  /**
   * Create a new instance of the Responder implementation depending on
   * whether the listener is being created for native Java NIO sockets
   * or for Netty Channels.
   *
   * @param server The server object for which the socket abstraction are
   *               being created.
   * @return An instance of the Responder interface implementation.
   * @throws IOException If an Exception occurs while creating a Responder
   *                     instance.
   */
  static Responder newInstance(Server server) throws IOException {
    return server.useNetty()
        ? new NettyResponder(server)
        : new NioResponder(server);
  }

  /**
   * Start the Responder instance.
   * <p>
   * NOTE: The responder is invoked in response to a RPC request. So the start
   * is more of a placeholder for now.
   */
  void start();

  /**
   * Interrupt active sockets and channels.
   */
  void interrupt();

  /**
   * Send a response to the RpcCall.
   *
   * @param call The RpcCall instance we are sending a response to.
   * @throws IOException If an exception occurs while responding to the
   *                     RpcCall.
   */
  void doRespond(Server.RpcCall call) throws IOException;
}
