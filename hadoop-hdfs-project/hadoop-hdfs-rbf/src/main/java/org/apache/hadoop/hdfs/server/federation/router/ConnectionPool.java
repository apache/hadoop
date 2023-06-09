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
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.ipc.AlignmentContext;
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
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
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
  /** Underlying socket index. **/
  private final AtomicInteger socketIndex = new AtomicInteger(0);

  /** Min number of connections per user. */
  private final int minSize;
  /** Max number of connections per user. */
  private final int maxSize;
  /** Min ratio of active connections per user. */
  private final float minActiveRatio;

  /** The last time a connection was active. */
  private volatile long lastActiveTime = 0;

  /** Enable using multiple physical socket or not. **/
  private final boolean enableMultiSocket;
  /** StateID alignment context. */
  private final PoolAlignmentContext alignmentContext;

  /** Map for the protocols and their protobuf implementations. */
  private final static Map<Class<?>, ProtoImpl> PROTO_MAP = new HashMap<>();
  static {
    PROTO_MAP.put(ClientProtocol.class,
        new ProtoImpl(ClientNamenodeProtocolPB.class,
            ClientNamenodeProtocolTranslatorPB.class));
    PROTO_MAP.put(NamenodeProtocol.class, new ProtoImpl(
        NamenodeProtocolPB.class, NamenodeProtocolTranslatorPB.class));
    PROTO_MAP.put(RefreshUserMappingsProtocol.class,
        new ProtoImpl(RefreshUserMappingsProtocolPB.class,
            RefreshUserMappingsProtocolClientSideTranslatorPB.class));
    PROTO_MAP.put(GetUserMappingsProtocol.class,
        new ProtoImpl(GetUserMappingsProtocolPB.class,
            GetUserMappingsProtocolClientSideTranslatorPB.class));
  }

  /** Class to store the protocol implementation. */
  private static class ProtoImpl {
    private final Class<?> protoPb;
    private final Class<?> protoClientPb;

    ProtoImpl(Class<?> pPb, Class<?> pClientPb) {
      this.protoPb = pPb;
      this.protoClientPb = pClientPb;
    }
  }

  protected ConnectionPool(Configuration config, String address,
      UserGroupInformation user, int minPoolSize, int maxPoolSize,
      float minActiveRatio, Class<?> proto, PoolAlignmentContext alignmentContext)
      throws IOException {

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
    this.minActiveRatio = minActiveRatio;
    this.enableMultiSocket = conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_NAMENODE_ENABLE_MULTIPLE_SOCKET_KEY,
        RBFConfigKeys.DFS_ROUTER_NAMENODE_ENABLE_MULTIPLE_SOCKET_DEFAULT);

    this.alignmentContext = alignmentContext;

    // Add minimum connections to the pool
    for (int i = 0; i < this.minSize; i++) {
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
   * Get the minimum ratio of active connections in this pool.
   *
   * @return Minimum ratio of active connections.
   */
  protected float getMinActiveRatio() {
    return this.minActiveRatio;
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
   * @return Client index.
   */
  @VisibleForTesting
  public AtomicInteger getClientIndex() {
    return this.clientIndex;
  }

  /**
   * Get the alignment context for this pool.
   * @return Alignment context
   */
  public PoolAlignmentContext getPoolAlignmentContext() {
    return this.alignmentContext;
  }

  /**
   * Return the next connection round-robin.
   *
   * @return Connection context.
   */
  protected ConnectionContext getConnection() {
    this.lastActiveTime = Time.now();
    List<ConnectionContext> tmpConnections = this.connections;
    for (ConnectionContext tmpConnection : tmpConnections) {
      if (tmpConnection != null && tmpConnection.isUsable()) {
        return tmpConnection;
      }
    }

    ConnectionContext conn = null;
    // We return a connection even if it's busy
    int size = tmpConnections.size();
    if (size > 0) {
      // Get a connection from the pool following round-robin
      // Inc and mask off sign bit, lookup index should be non-negative int
      int threadIndex = this.clientIndex.getAndIncrement() & 0x7FFFFFFF;
      conn = tmpConnections.get(threadIndex % size);
    }
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
    if (this.connections.size() > this.minSize) {
      int targetCount = Math.min(num, this.connections.size() - this.minSize);
      // Remove and close targetCount of connections
      List<ConnectionContext> tmpConnections = new ArrayList<>();
      for (ConnectionContext conn : this.connections) {
        // Only pick idle connections to close
        if (removed.size() < targetCount && conn.isIdle()) {
          removed.add(conn);
        } else {
          tmpConnections.add(conn);
        }
      }
      this.connections = tmpConnections;
    }
    LOG.debug("Expected to remove {} connection and actually removed {} connections "
        + "for connectionPool: {}", num, removed.size(), connectionPoolId);
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
      connection.close(true);
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
   * Number of usable i.e. no active thread connections.
   *
   * @return Number of idle connections
   */
  protected int getNumIdleConnections() {
    int ret = 0;
    List<ConnectionContext> tmpConnections = this.connections;
    for (ConnectionContext conn : tmpConnections) {
      if (conn.isIdle()) {
        ret++;
      }
    }
    return ret;
  }

  /**
   * Number of active connections recently in the pool.
   *
   * @return Number of active connections recently.
   */
  protected int getNumActiveConnectionsRecently() {
    int ret = 0;
    List<ConnectionContext> tmpConnections = this.connections;
    for (ConnectionContext conn : tmpConnections) {
      if (conn.isActiveRecently()) {
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
    info.put("recent_active",
        Integer.toString(getNumActiveConnectionsRecently()));
    info.put("idle", Integer.toString(getNumIdleConnections()));
    info.put("total", Integer.toString(getNumConnections()));
    if (LOG.isDebugEnabled()) {
      List<ConnectionContext> tmpConnections = this.connections;
      for (int i=0; i<tmpConnections.size(); i++) {
        ConnectionContext connection = tmpConnections.get(i);
        info.put(i + " active", Boolean.toString(connection.isActive()));
        info.put(i + " recent_active",
            Integer.toString(getNumActiveConnectionsRecently()));
        info.put(i + " idle", Boolean.toString(connection.isUsable()));
        info.put(i + " closed", Boolean.toString(connection.isClosed()));
      }
    }
    return JSON.toString(info);
  }

  /**
   * Create a new proxy wrapper for a client NN connection.
   * @return Proxy for the target ClientProtocol that contains the user's
   *         security context.
   * @throws IOException If it cannot get a new connection.
   */
  public ConnectionContext newConnection() throws IOException {
    return newConnection(this.conf, this.namenodeAddress,
        this.ugi, this.protocol, this.enableMultiSocket,
        this.socketIndex.incrementAndGet(), alignmentContext);
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
   * @param enableMultiSocket Enable multiple socket or not.
   * @param socketIndex Index for FederationConnectionId.
   * @param alignmentContext Client alignment context.
   * @param <T> Input type T.
   * @return proto for the target ClientProtocol that contains the user's
   * security context.
   * @throws IOException If it cannot be created.
   */
  protected static <T> ConnectionContext newConnection(Configuration conf,
      String nnAddress, UserGroupInformation ugi, Class<T> proto,
      boolean enableMultiSocket, int socketIndex,
      AlignmentContext alignmentContext) throws IOException {
    if (!PROTO_MAP.containsKey(proto)) {
      String msg = "Unsupported protocol for connection to NameNode: "
          + ((proto != null) ? proto.getName() : "null");
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }
    ProtoImpl classes = PROTO_MAP.get(proto);
    RPC.setProtocolEngine(conf, classes.protoPb, ProtobufRpcEngine2.class);

    final RetryPolicy defaultPolicy = RetryUtils.getDefaultRetryPolicy(conf,
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
    final long version = RPC.getProtocolVersion(classes.protoPb);
    Object proxy;
    if (enableMultiSocket) {
      FederationConnectionId connectionId = new FederationConnectionId(
          socket, classes.protoPb, ugi, RPC.getRpcTimeout(conf),
          defaultPolicy, conf, socketIndex);
      proxy = RPC.getProtocolProxy(classes.protoPb, version, connectionId,
          conf, factory, alignmentContext).getProxy();
    } else {
      proxy = RPC.getProtocolProxy(classes.protoPb, version, socket, ugi,
          conf, factory, RPC.getRpcTimeout(conf), defaultPolicy, null,
          alignmentContext).getProxy();
    }

    T client = newProtoClient(proto, classes, proxy);
    Text dtService = SecurityUtil.buildTokenService(socket);

    ProxyAndInfo<T> clientProxy = new ProxyAndInfo<T>(client, dtService, socket);
    return new ConnectionContext(clientProxy, conf);
  }

  private static <T> T newProtoClient(Class<T> proto, ProtoImpl classes,
      Object proxy) {
    try {
      Constructor<?> constructor =
          classes.protoClientPb.getConstructor(classes.protoPb);
      Object o = constructor.newInstance(proxy);
      if (proto.isAssignableFrom(o.getClass())) {
        @SuppressWarnings("unchecked")
        T client = (T) o;
        return client;
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
    return null;
  }
}