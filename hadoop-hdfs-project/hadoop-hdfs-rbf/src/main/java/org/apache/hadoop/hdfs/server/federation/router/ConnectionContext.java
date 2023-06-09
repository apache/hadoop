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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG =
      LoggerFactory.getLogger(ConnectionContext.class);

  /** Client for the connection. */
  private final ProxyAndInfo<?> client;
  /** How many threads are using this connection. */
  private int numThreads = 0;
  /** If the connection is closed. */
  private boolean closed = false;
  /** Last timestamp the connection was active. */
  private long lastActiveTs = 0;
  /** The connection's active status would expire after this window. */
  private final static long ACTIVE_WINDOW_TIME = TimeUnit.SECONDS.toMillis(30);
  /** The maximum number of requests that this connection can handle concurrently. **/
  private final int maxConcurrencyPerConn;

  public ConnectionContext(ProxyAndInfo<?> connection, Configuration conf) {
    this.client = connection;
    this.maxConcurrencyPerConn = conf.getInt(
        RBFConfigKeys.DFS_ROUTER_MAX_CONCURRENCY_PER_CONNECTION_KEY,
        RBFConfigKeys.DFS_ROUTER_MAX_CONCURRENCY_PER_CONNECTION_DEFAULT);
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
   * Check if the connection is/was active recently.
   *
   * @return True if the connection is active or
   * was active in the past period of time.
   */
  public synchronized boolean isActiveRecently() {
    return Time.monotonicNow() - this.lastActiveTs <= ACTIVE_WINDOW_TIME;
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
    return hasAvailableConcurrency() && !isClosed();
  }

  /**
   * Return true if this connection context still has available concurrency,
   * else return false.
   */
  private synchronized boolean hasAvailableConcurrency() {
    return this.numThreads < maxConcurrencyPerConn;
  }

  /**
   *  Check if the connection is idle. It checks if the connection is not used
   *  by another thread.
   * @return True if the connection is not used by another thread.
   */
  public synchronized boolean isIdle() {
    return !isActive() && !isClosed();
  }

  /**
   * Get the connection client.
   *
   * @return Connection client.
   */
  public synchronized ProxyAndInfo<?> getClient() {
    this.numThreads++;
    this.lastActiveTs = Time.monotonicNow();
    return this.client;
  }

  /**
   * Release this connection.
   */
  public synchronized void release() {
    if (this.numThreads > 0) {
      this.numThreads--;
    }
  }

  /**
   * Close a connection. Only idle connections can be closed since
   * the RPC proxy would be shut down immediately.
   *
   * @param force whether the connection should be closed anyway.
   */
  public synchronized void close(boolean force) {
    if (!force && this.numThreads > 0) {
      // this is an erroneous case, but we have to close the connection
      // anyway since there will be connection leak if we don't do so
      // the connection has been moved out of the pool
      LOG.error("Active connection with {} handlers will be closed, ConnectionContext is {}",
          this.numThreads, this);
    }
    this.closed = true;
    Object proxy = this.client.getProxy();
    // Nobody should be using this anymore, so it should close right away
    RPC.stopProxy(proxy);
  }

  public synchronized void close() {
    close(false);
  }

  @Override
  public String toString() {
    InetSocketAddress addr = this.client.getAddress();
    Object proxy = this.client.getProxy();
    Class<?> clazz = proxy.getClass();

    StringBuilder sb = new StringBuilder();
    sb.append("hashcode:")
        .append(hashCode())
        .append(" ")
        .append(clazz.getSimpleName())
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