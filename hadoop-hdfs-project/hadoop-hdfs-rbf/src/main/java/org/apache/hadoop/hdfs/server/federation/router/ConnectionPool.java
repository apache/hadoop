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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains a pool of connections for each User (including tokens) + NN. The
 * RPC client maintains a single socket, to achieve throughput similar to a NN,
 * each request is multiplexed across multiple sockets/connections from a
 * pool.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ConnectionPool {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConnectionPool.class);


  /** Configuration settings for the connection pool. */
  private final Configuration conf;

  /** Identifier for this connection pool. */
  private final ConnectionPoolId connectionPoolId;
  /** Namenode this pool connects to. */
  private final String namenodeAddress;
  /** User for this connections. */
  private final UserGroupInformation ugi;
  /** Class of the protocol. */
  private final Class<?> protocol;

  /** Pool of connections. We mimic a COW array. */
  private volatile List<ConnectionContext> connections = new ArrayList<>();
  /** Connection index for round-robin. */
  private final AtomicInteger clientIndex = new AtomicInteger(0);

  /** Min number of connections per user. */
  private final int minSize;
  /** Max number of connections per user. */
  private final int maxSize;

  /** The last time a connection was active. */
  private volatile long lastActiveTime = 0;


  protected ConnectionPool(Configuration config, String address,
      UserGroupInformation user, int minPoolSize, int maxPoolSize,
      Class<?> proto) throws IOException {

    this.conf = config;

    // Connection pool target
    this.ugi = user;
    this.namenodeAddress = address;
    this.protocol = proto;
    this.connectionPoolId =
        new ConnectionPoolId(this.ugi, this.namenodeAddress, this.protocol);

    // Set configuration parameters for the pool
    this.minSize = minPoolSize;
    this.maxSize = maxPoolSize;

    // Add minimum connections to the pool
    for (int i=0; i<this.minSize; i++) {
      ConnectionContext newConnection = newConnection();
      this.connections.add(newConnection);
    }
    LOG.debug("Created connection pool \"{}\" with {} connections",
        this.connectionPoolId, this.minSize);
  }

  /**
   * Get the maximum number of connections allowed in this pool.
   *
   * @return Maximum number of connections.
   */
  protected int getMaxSize() {
    return this.maxSize;
  }

  /**
   * Get the minimum number of connections in this pool.
   *
   * @return Minimum number of connections.
   */
  protected int getMinSize() {
    return this.minSize;
  }

  /**
   * Get the connection pool identifier.
   *
   * @return Connection pool identifier.
   */
  protected ConnectionPoolId getConnectionPoolId() {
    return this.connectionPoolId;
  }

  /**
   * Get the clientIndex used to calculate index for lookup.
   */
  @VisibleForTesting
  public AtomicInteger getClientIndex() {
    return this.clientIndex;
  }

  /**
   * Return the next connection round-robin.
   *
   * @return Connection context.
   */
  protected ConnectionContext getConnection() {

    this.lastActiveTime = Time.now();

    // Get a connection from the pool following round-robin
    ConnectionContext conn = null;
    List<ConnectionContext> tmpConnections = this.connections;
    int size = tmpConnections.size();
    // Inc and mask off sign bit, lookup index should be non-negative int
    int threadIndex = this.clientIndex.getAndIncrement() & 0x7FFFFFFF;
    for (int i=0; i<size; i++) {
      int index = (threadIndex + i) % size;
      conn = tmpConnections.get(index);
      if (conn != null && conn.isUsable()) {
        return conn;
      }
    }

    // We return a connection even if it's active
    return conn;
  }

  /**
   * Add a connection to the current pool. It uses a Copy-On-Write approach.
   *
   * @param conn New connection to add to the pool.
   */
  public synchronized void addConnection(ConnectionContext conn) {
    List<ConnectionContext> tmpConnections = new ArrayList<>(this.connections);
    tmpConnections.add(conn);
    this.connections = tmpConnections;

    this.lastActiveTime = Time.now();
  }

  /**
   * Remove connections from the current pool.
   *
   * @param num Number of connections to remove.
   * @return Removed connections.
   */
  public synchronized List<ConnectionContext> removeConnections(int num) {
    List<ConnectionContext> removed = new LinkedList<>();

    // Remove and close the last connection
    List<ConnectionContext> tmpConnections = new ArrayList<>();
    for (int i=0; i<this.connections.size(); i++) {
      ConnectionContext conn = this.connections.get(i);
      if (i < this.minSize || i < this.connections.size() - num) {
        tmpConnections.add(conn);
      } else {
        removed.add(conn);
      }
    }
    this.connections = tmpConnections;

    return removed;
  }

  /**
   * Close the connection pool.
   */
  protected synchronized void close() {
    long timeSinceLastActive = TimeUnit.MILLISECONDS.toSeconds(
        Time.now() - getLastActiveTime());
    LOG.debug("Shutting down connection pool \"{}\" used {} seconds ago",
        this.connectionPoolId, timeSinceLastActive);

    for (ConnectionContext connection : this.connections) {
      connection.close();
    }
    this.connections.clear();
  }

  /**
   * Number of connections in the pool.
   *
   * @return Number of connections.
   */
  protected int getNumConnections() {
    return this.connections.size();
  }

  /**
   * Number of active connections in the pool.
   *
   * @return Number of active connections.
   */
  protected int getNumActiveConnections() {
    int ret = 0;

    List<ConnectionContext> tmpConnections = this.connections;
    for (ConnectionContext conn : tmpConnections) {
      if (conn.isActive()) {
        ret++;
      }
    }
    return ret;
  }

  /**
   * Get the last time the connection pool was used.
   *
   * @return Last time the connection pool was used.
   */
  protected long getLastActiveTime() {
    return this.lastActiveTime;
  }

  @Override
  public String toString() {
    return this.connectionPoolId.toString();
  }

  /**
   * JSON representation of the connection pool.
   *
   * @return String representation of the JSON.
   */
  public String getJSON() {
    final Map<String, String> info = new LinkedHashMap<>();
    info.put("active", Integer.toString(getNumActiveConnections()));
    info.put("total", Integer.toString(getNumConnections()));
    if (LOG.isDebugEnabled()) {
      List<ConnectionContext> tmpConnections = this.connections;
      for (int i=0; i<tmpConnections.size(); i++) {
        ConnectionContext connection = tmpConnections.get(i);
        info.put(i + " active", Boolean.toString(connection.isActive()));
        info.put(i + " closed", Boolean.toString(connection.isClosed()));
      }
    }
    return JSON.toString(info);
  }

  /**
   * Create a new proxy wrapper for a client NN connection.
   * @return Proxy for the target ClientProtocol that contains the user's
   *         security context.
   * @throws IOException
   */
  public ConnectionContext newConnection() throws IOException {
    return newConnection(
        this.conf, this.namenodeAddress, this.ugi, this.protocol);
  }

  /**
   * Creates a proxy wrapper for a client NN connection. Each proxy contains
   * context for a single user/security context. To maximize throughput it is
   * recommended to use multiple connection per user+server, allowing multiple
   * writes and reads to be dispatched in parallel.
   *
   * @param conf Configuration for the connection.
   * @param nnAddress Address of server supporting the ClientProtocol.
   * @param ugi User context.
   * @param proto Interface of the protocol.
   * @return proto for the target ClientProtocol that contains the user's
   *         security context.
   * @throws IOException If it cannot be created.
   */
  protected static ConnectionContext newConnection(Configuration conf,
      String nnAddress, UserGroupInformation ugi, Class<?> proto)
          throws IOException {
    ConnectionContext ret;
    if (proto == ClientProtocol.class) {
      ret = newClientConnection(conf, nnAddress, ugi);
    } else if (proto == NamenodeProtocol.class) {
      ret = newNamenodeConnection(conf, nnAddress, ugi);
    } else {
      String msg = "Unsupported protocol for connection to NameNode: " +
          ((proto != null) ? proto.getClass().getName() : "null");
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }
    return ret;
  }

  /**
   * Creates a proxy wrapper for a client NN connection. Each proxy contains
   * context for a single user/security context. To maximize throughput it is
   * recommended to use multiple connection per user+server, allowing multiple
   * writes and reads to be dispatched in parallel.
   *
   * Mostly based on NameNodeProxies#createNonHAProxy() but it needs the
   * connection identifier.
   *
   * @param conf Configuration for the connection.
   * @param nnAddress Address of server supporting the ClientProtocol.
   * @param ugi User context.
   * @return Proxy for the target ClientProtocol that contains the user's
   *         security context.
   * @throws IOException If it cannot be created.
   */
  private static ConnectionContext newClientConnection(
      Configuration conf, String nnAddress, UserGroupInformation ugi)
          throws IOException {
    RPC.setProtocolEngine(
        conf, ClientNamenodeProtocolPB.class, ProtobufRpcEngine.class);

    final RetryPolicy defaultPolicy = RetryUtils.getDefaultRetryPolicy(
        conf,
        HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY,
        HdfsClientConfigKeys.Retry.POLICY_ENABLED_DEFAULT,
        HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY,
        HdfsClientConfigKeys.Retry.POLICY_SPEC_DEFAULT,
        HdfsConstants.SAFEMODE_EXCEPTION_CLASS_NAME);

    SocketFactory factory = SocketFactory.getDefault();
    if (UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
    }
    InetSocketAddress socket = NetUtils.createSocketAddr(nnAddress);
    final long version = RPC.getProtocolVersion(ClientNamenodeProtocolPB.class);
    ClientNamenodeProtocolPB proxy = RPC.getProtocolProxy(
        ClientNamenodeProtocolPB.class, version, socket, ugi, conf,
        factory, RPC.getRpcTimeout(conf), defaultPolicy, null).getProxy();
    ClientProtocol client = new ClientNamenodeProtocolTranslatorPB(proxy);
    Text dtService = SecurityUtil.buildTokenService(socket);

    ProxyAndInfo<ClientProtocol> clientProxy =
        new ProxyAndInfo<ClientProtocol>(client, dtService, socket);
    ConnectionContext connection = new ConnectionContext(clientProxy);
    return connection;
  }

  /**
   * Creates a proxy wrapper for a NN connection. Each proxy contains context
   * for a single user/security context. To maximize throughput it is
   * recommended to use multiple connection per user+server, allowing multiple
   * writes and reads to be dispatched in parallel.
   *
   * @param conf Configuration for the connection.
   * @param nnAddress Address of server supporting the ClientProtocol.
   * @param ugi User context.
   * @return Proxy for the target NamenodeProtocol that contains the user's
   *         security context.
   * @throws IOException If it cannot be created.
   */
  private static ConnectionContext newNamenodeConnection(
      Configuration conf, String nnAddress, UserGroupInformation ugi)
          throws IOException {
    RPC.setProtocolEngine(
        conf, NamenodeProtocolPB.class, ProtobufRpcEngine.class);

    final RetryPolicy defaultPolicy = RetryUtils.getDefaultRetryPolicy(
        conf,
        HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY,
        HdfsClientConfigKeys.Retry.POLICY_ENABLED_DEFAULT,
        HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY,
        HdfsClientConfigKeys.Retry.POLICY_SPEC_DEFAULT,
        HdfsConstants.SAFEMODE_EXCEPTION_CLASS_NAME);

    SocketFactory factory = SocketFactory.getDefault();
    if (UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
    }
    InetSocketAddress socket = NetUtils.createSocketAddr(nnAddress);
    final long version = RPC.getProtocolVersion(NamenodeProtocolPB.class);
    NamenodeProtocolPB proxy = RPC.getProtocolProxy(NamenodeProtocolPB.class,
        version, socket, ugi, conf,
        factory, RPC.getRpcTimeout(conf), defaultPolicy, null).getProxy();
    NamenodeProtocol client = new NamenodeProtocolTranslatorPB(proxy);
    Text dtService = SecurityUtil.buildTokenService(socket);

    ProxyAndInfo<NamenodeProtocol> clientProxy =
        new ProxyAndInfo<NamenodeProtocol>(client, dtService, socket);
    ConnectionContext connection = new ConnectionContext(clientProxy);
    return connection;
  }
}
