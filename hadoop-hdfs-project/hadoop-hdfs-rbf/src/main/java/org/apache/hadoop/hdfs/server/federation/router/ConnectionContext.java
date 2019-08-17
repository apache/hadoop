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
package org.apache.hadoop.hdfs.server.federation.router;

import java.net.InetSocketAddress;

import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.ipc.RPC;

/**
 * Context to track a connection in a {@link ConnectionPool}. When a client uses
 * a connection, it increments a counter to mark it as active. Once the client
 * is done with the connection, it decreases the counter. It also takes care of
 * closing the connection once is not active.
 *
 * The protocols currently used are:
 * <ul>
 * <li>{@link org.apache.hadoop.hdfs.protocol.ClientProtocol}
 * <li>{@link org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol}
 * </ul>
 */
public class ConnectionContext {

  /** Client for the connection. */
  private final ProxyAndInfo<?> client;
  /** How many threads are using this connection. */
  private int numThreads = 0;
  /** If the connection is closed. */
  private boolean closed = false;


  public ConnectionContext(ProxyAndInfo<?> connection) {
    this.client = connection;
  }

  /**
   * Check if the connection is active.
   *
   * @return True if the connection is active.
   */
  public synchronized boolean isActive() {
    return this.numThreads > 0;
  }

  /**
   * Check if the connection is closed.
   *
   * @return If the connection is closed.
   */
  public synchronized boolean isClosed() {
    return this.closed;
  }

  /**
   * Check if the connection can be used. It checks if the connection is used by
   * another thread or already closed.
   *
   * @return True if the connection can be used.
   */
  public synchronized boolean isUsable() {
    return !isActive() && !isClosed();
  }

  /**
   * Get the connection client.
   *
   * @return Connection client.
   */
  public synchronized ProxyAndInfo<?> getClient() {
    this.numThreads++;
    return this.client;
  }

  /**
   * Release this connection. If the connection was closed, close the proxy.
   * Otherwise, mark the connection as not used by us anymore.
   */
  public synchronized void release() {
    if (--this.numThreads == 0 && this.closed) {
      close();
    }
  }

  /**
   * We will not use this connection anymore. If it's not being used, we close
   * it. Otherwise, we let release() do it once we are done with it.
   */
  public synchronized void close() {
    this.closed = true;
    if (this.numThreads == 0) {
      Object proxy = this.client.getProxy();
      // Nobody should be using this anymore so it should close right away
      RPC.stopProxy(proxy);
    }
  }

  @Override
  public String toString() {
    InetSocketAddress addr = this.client.getAddress();
    Object proxy = this.client.getProxy();
    Class<?> clazz = proxy.getClass();

    StringBuilder sb = new StringBuilder();
    sb.append(clazz.getSimpleName())
        .append("@")
        .append(addr)
        .append("x")
        .append(numThreads);
    if (closed) {
      sb.append("[CLOSED]");
    }
    return sb.toString();
  }
}