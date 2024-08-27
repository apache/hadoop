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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SEPARATOR_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_IP_PROXY_USERS;
import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCPerformanceMonitor.CONCURRENT;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.ha.ReadOnly;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A client proxy for Router to NN communication using the NN ClientProtocol.
 * <p>
 * Provides routers to invoke remote ClientProtocol methods and handle
 * retries/failover.
 * <ul>
 * <li>invokeSingle Make a single request to a single namespace
 * <li>invokeSequential Make a sequential series of requests to multiple
 * ordered namespaces until a condition is met.
 * <li>invokeConcurrent Make concurrent requests to multiple namespaces and
 * return all of the results.
 * </ul>
 * Also maintains a cached pool of connections to NNs. Connections are managed
 * by the ConnectionManager and are unique to each user + NN. The size of the
 * connection pool can be configured. Larger pools allow for more simultaneous
 * requests to a single NN from a single user.
 */
public class RouterRpcClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterRpcClient.class);


  /** Router using this RPC client. */
  private final Router router;

  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private final ActiveNamenodeResolver namenodeResolver;

  /** Connection pool to the Namenodes per user for performance. */
  private final ConnectionManager connectionManager;
  /** Service to run asynchronous calls. */
  private final ThreadPoolExecutor executorService;
  /** Retry policy for router -> NN communication. */
  private final RetryPolicy retryPolicy;
  /** Optional perf monitor. */
  private final RouterRpcMonitor rpcMonitor;
  /** Field separator of CallerContext. */
  private final String contextFieldSeparator;
  /** Observer read enabled. Default for all nameservices. */
  private final boolean observerReadEnabledDefault;
  /** Nameservice specific overrides of the default setting for enabling observer reads. */
  private HashSet<String> observerReadEnabledOverrides = new HashSet<>();
  /**
   * Period to refresh namespace stateID using active namenode.
   * This ensures the namespace stateID is fresh even when an
   * observer is trailing behind.
   */
  private long activeNNStateIdRefreshPeriodMs;
  /** Last msync times for each namespace. */
  private final ConcurrentHashMap<String, LongAccumulator> lastActiveNNRefreshTimes;

  /** Pattern to parse a stack trace line. */
  private static final Pattern STACK_TRACE_PATTERN =
      Pattern.compile("\\tat (.*)\\.(.*)\\((.*):(\\d*)\\)");

  /** Fairness manager to control handlers assigned per NS. */
  private volatile RouterRpcFairnessPolicyController routerRpcFairnessPolicyController;
  private Map<String, LongAdder> rejectedPermitsPerNs = new ConcurrentHashMap<>();
  private Map<String, LongAdder> acceptedPermitsPerNs = new ConcurrentHashMap<>();

  private final boolean enableProxyUser;

  /**
   * Create a router RPC client to manage remote procedure calls to NNs.
   *
   * @param conf Hdfs Configuration.
   * @param router A router using this RPC client.
   * @param resolver A NN resolver to determine the currently active NN in HA.
   * @param monitor Optional performance monitor.
   * @param routerStateIdContext the router state context object to hold the state ids for all
   * namespaces.
   */
  public RouterRpcClient(Configuration conf, Router router,
      ActiveNamenodeResolver resolver, RouterRpcMonitor monitor,
      RouterStateIdContext routerStateIdContext) {
    this.router = router;

    this.namenodeResolver = resolver;

    Configuration clientConf = getClientConfiguration(conf);
    this.contextFieldSeparator =
        clientConf.get(HADOOP_CALLER_CONTEXT_SEPARATOR_KEY,
            HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT);
    this.connectionManager = new ConnectionManager(clientConf, routerStateIdContext);
    this.connectionManager.start();
    this.routerRpcFairnessPolicyController =
        FederationUtil.newFairnessPolicyController(conf);

    int numThreads = conf.getInt(
        RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE,
        RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE_DEFAULT);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("RPC Router Client-%d")
        .build();
    BlockingQueue<Runnable> workQueue;
    if (conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_CLIENT_REJECT_OVERLOAD,
        RBFConfigKeys.DFS_ROUTER_CLIENT_REJECT_OVERLOAD_DEFAULT)) {
      workQueue = new ArrayBlockingQueue<>(numThreads);
    } else {
      workQueue = new LinkedBlockingQueue<>();
    }
    this.executorService = new ThreadPoolExecutor(numThreads, numThreads,
        0L, TimeUnit.MILLISECONDS, workQueue, threadFactory);

    this.rpcMonitor = monitor;

    int maxFailoverAttempts = conf.getInt(
        HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_KEY,
        HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_DEFAULT);
    int maxRetryAttempts = conf.getInt(
        RBFConfigKeys.DFS_ROUTER_CLIENT_MAX_ATTEMPTS,
        RBFConfigKeys.DFS_ROUTER_CLIENT_MAX_ATTEMPTS_DEFAULT);
    int failoverSleepBaseMillis = conf.getInt(
        HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_KEY,
        HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_DEFAULT);
    int failoverSleepMaxMillis = conf.getInt(
        HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_KEY,
        HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_DEFAULT);
    this.retryPolicy = RetryPolicies.failoverOnNetworkException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, maxFailoverAttempts, maxRetryAttempts,
        failoverSleepBaseMillis, failoverSleepMaxMillis);
    String[] ipProxyUsers = conf.getStrings(DFS_NAMENODE_IP_PROXY_USERS);
    this.enableProxyUser = ipProxyUsers != null && ipProxyUsers.length > 0;
    this.observerReadEnabledDefault = conf.getBoolean(
        RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY,
        RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_VALUE);
    String[] observerReadOverrides =
        conf.getStrings(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_OVERRIDES);
    if (observerReadOverrides != null) {
      observerReadEnabledOverrides.addAll(Arrays.asList(observerReadOverrides));
    }
    if (this.observerReadEnabledDefault) {
      LOG.info("Observer read is enabled for router.");
    }
    this.activeNNStateIdRefreshPeriodMs = conf.getTimeDuration(
        RBFConfigKeys.DFS_ROUTER_OBSERVER_STATE_ID_REFRESH_PERIOD_KEY,
        RBFConfigKeys.DFS_ROUTER_OBSERVER_STATE_ID_REFRESH_PERIOD_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    if (activeNNStateIdRefreshPeriodMs < 0) {
      LOG.info("Periodic stateId freshness check is disabled"
              + " since '{}' is {}ms, which is less than 0.",
          RBFConfigKeys.DFS_ROUTER_OBSERVER_STATE_ID_REFRESH_PERIOD_KEY,
          activeNNStateIdRefreshPeriodMs);
    }
    this.lastActiveNNRefreshTimes = new ConcurrentHashMap<>();
  }

  /**
   * Get the configuration for the RPC client. It takes the Router
   * configuration and transforms it into regular RPC Client configuration.
   * @param conf Input configuration.
   * @return Configuration for the RPC client.
   */
  private Configuration getClientConfiguration(final Configuration conf) {
    Configuration clientConf = new Configuration(conf);
    int maxRetries = conf.getInt(
        RBFConfigKeys.DFS_ROUTER_CLIENT_MAX_RETRIES_TIME_OUT,
        RBFConfigKeys.DFS_ROUTER_CLIENT_MAX_RETRIES_TIME_OUT_DEFAULT);
    if (maxRetries >= 0) {
      clientConf.setInt(
          IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, maxRetries);
    }
    long connectTimeOut = conf.getTimeDuration(
        RBFConfigKeys.DFS_ROUTER_CLIENT_CONNECT_TIMEOUT,
        RBFConfigKeys.DFS_ROUTER_CLIENT_CONNECT_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    if (connectTimeOut >= 0) {
      clientConf.setLong(IPC_CLIENT_CONNECT_TIMEOUT_KEY, connectTimeOut);
    }
    return clientConf;
  }

  /**
   * Get the active namenode resolver used by this client.
   * @return Active namenode resolver.
   */
  public ActiveNamenodeResolver getNamenodeResolver() {
    return this.namenodeResolver;
  }

  /**
   * Shutdown the client.
   */
  public void shutdown() {
    if (this.connectionManager != null) {
      this.connectionManager.close();
    }
    if (this.executorService != null) {
      this.executorService.shutdownNow();
    }
    if (this.routerRpcFairnessPolicyController != null) {
      this.routerRpcFairnessPolicyController.shutdown();
    }
  }

  /**
   * Total number of available sockets between the router and NNs.
   *
   * @return Number of namenode clients.
   */
  public int getNumConnections() {
    return this.connectionManager.getNumConnections();
  }

  /**
   * Total number of available sockets between the router and NNs.
   *
   * @return Number of namenode clients.
   */
  public int getNumActiveConnections() {
    return this.connectionManager.getNumActiveConnections();
  }

  /**
   * Total number of idle sockets between the router and NNs.
   *
   * @return Number of namenode clients.
   */
  public int getNumIdleConnections() {
    return this.connectionManager.getNumIdleConnections();
  }

  /**
   * Total number of active sockets between the router and NNs.
   *
   * @return Number of recently active namenode clients.
   */
  public int getNumActiveConnectionsRecently() {
    return this.connectionManager.getNumActiveConnectionsRecently();
  }

  /**
   * Total number of open connection pools to a NN. Each connection pool.
   * represents one user + one NN.
   *
   * @return Number of connection pools.
   */
  public int getNumConnectionPools() {
    return this.connectionManager.getNumConnectionPools();
  }

  /**
   * Number of connections between the router and NNs being created sockets.
   *
   * @return Number of connections waiting to be created.
   */
  public int getNumCreatingConnections() {
    return this.connectionManager.getNumCreatingConnections();
  }

  /**
   * JSON representation of the connection pool.
   *
   * @return String representation of the JSON.
   */
  public String getJSON() {
    return this.connectionManager.getJSON();
  }

  /**
   * JSON representation of the async caller thread pool.
   *
   * @return String representation of the JSON.
   */
  public String getAsyncCallerPoolJson() {
    final Map<String, Integer> info = new LinkedHashMap<>();
    info.put("active", executorService.getActiveCount());
    info.put("total", executorService.getPoolSize());
    info.put("max", executorService.getMaximumPoolSize());
    return JSON.toString(info);
  }

  /**
   * JSON representation of the rejected permits for each nameservice.
   *
   * @return String representation of the rejected permits for each nameservice.
   */
  public String getRejectedPermitsPerNsJSON() {
    return JSON.toString(rejectedPermitsPerNs);
  }

  /**
   * JSON representation of the accepted permits for each nameservice.
   *
   * @return String representation of the accepted permits for each nameservice.
   */
  public String getAcceptedPermitsPerNsJSON() {
    return JSON.toString(acceptedPermitsPerNs);
  }
  /**
   * Get ClientProtocol proxy client for a NameNode. Each combination of user +
   * NN must use a unique proxy client. Previously created clients are cached
   * and stored in a connection pool by the ConnectionManager.
   *
   * @param ugi User group information.
   * @param nsId Nameservice identifier.
   * @param rpcAddress RPC server address of the NN.
   * @param proto Protocol of the connection.
   * @return ConnectionContext containing a ClientProtocol proxy client for the
   *         NN + current user.
   * @throws IOException If we cannot get a connection to the NameNode.
   */
  protected ConnectionContext getConnection(UserGroupInformation ugi, String nsId,
      String rpcAddress, Class<?> proto) throws IOException {
    ConnectionContext connection = null;
    try {
      // Each proxy holds the UGI info for the current user when it is created.
      // This cache does not scale very well, one entry per user per namenode,
      // and may need to be adjusted and/or selectively pruned. The cache is
      // important due to the excessive overhead of creating a new proxy wrapper
      // for each individual request.

      // TODO Add tokens from the federated UGI
      UserGroupInformation connUGI = ugi;
      if (UserGroupInformation.isSecurityEnabled() || this.enableProxyUser) {
        UserGroupInformation routerUser = UserGroupInformation.getLoginUser();
        connUGI = UserGroupInformation.createProxyUser(
            ugi.getUserName(), routerUser);
      }
      connection = this.connectionManager.getConnection(
          connUGI, rpcAddress, proto, nsId);
      LOG.debug("User {} NN {} is using connection {}",
          ugi.getUserName(), rpcAddress, connection);
    } catch (Exception ex) {
      LOG.error("Cannot open NN client to address: {}", rpcAddress, ex);
    }

    if (connection == null) {
      throw new ConnectionNullException("Cannot get a connection to "
          + rpcAddress);
    }
    return connection;
  }

  /**
   * Convert an exception to an IOException.
   *
   * For a non-IOException, wrap it with IOException. For a RemoteException,
   * unwrap it. For an IOException which is not a RemoteException, return it.
   *
   * @param e Exception to convert into an exception.
   * @return Created IO exception.
   */
  private static IOException toIOException(Exception e) {
    if (e instanceof RemoteException) {
      return ((RemoteException) e).unwrapRemoteException();
    }
    if (e instanceof IOException) {
      return (IOException)e;
    }
    return new IOException(e);
  }

  /**
   * If we should retry the RPC call.
   *
   * @param ioe IOException reported.
   * @param retryCount Number of retries.
   * @param nsId Nameservice ID.
   * @param namenode namenode context.
   * @param listObserverFirst Observer read case, observer NN will be ranked first.
   * @return Retry decision.
   * @throws IOException An IO Error occurred.
   */
  protected RetryDecision shouldRetry(
      final IOException ioe, final int retryCount, final String nsId,
      final FederationNamenodeContext namenode,
      final boolean listObserverFirst) throws IOException {
    // check for the case of cluster unavailable state
    if (isClusterUnAvailable(nsId, namenode, listObserverFirst)) {
      // we allow to retry once if cluster is unavailable
      if (retryCount == 0) {
        return RetryDecision.RETRY;
      } else {
        throw new NoNamenodesAvailableException(nsId, ioe);
      }
    }

    try {
      final RetryPolicy.RetryAction a =
          this.retryPolicy.shouldRetry(ioe, retryCount, 0, true);
      return a.action;
    } catch (Exception ex) {
      LOG.error("Re-throwing API exception, no more retries", ex);
      throw toIOException(ex);
    }
  }

  /**
   * Invokes a method against the ClientProtocol proxy server. If a standby
   * exception is generated by the call to the client, retries using the
   * alternate server.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param ugi User group information.
   * @param namenodes A prioritized list of namenodes within the same
   *                  nameservice.
   * @param useObserver Whether to use observer namenodes.
   * @param protocol the protocol of the connection.
   * @param method Remote ClientProtocol method to invoke.
   * @param params Variable list of parameters matching the method.
   * @return The result of invoking the method.
   * @throws ConnectException If it cannot connect to any Namenode.
   * @throws StandbyException If all Namenodes are in Standby.
   * @throws IOException If it cannot invoke the method.
   */
  @VisibleForTesting
  public Object invokeMethod(
      final UserGroupInformation ugi,
      final List<? extends FederationNamenodeContext> namenodes,
      boolean useObserver,
      final Class<?> protocol, final Method method, final Object... params)
          throws ConnectException, StandbyException, IOException {

    if (namenodes == null || namenodes.isEmpty()) {
      throw new IOException("No namenodes to invoke " + method.getName() +
          " with params " + Arrays.deepToString(params) + " from "
          + router.getRouterId());
    }

    addClientInfoToCallerContext(ugi);

    Object ret = null;
    if (rpcMonitor != null) {
      rpcMonitor.proxyOp();
    }

    ExecutionStatus status = new ExecutionStatus(false, useObserver);
    Map<FederationNamenodeContext, IOException> ioes = new LinkedHashMap<>();
    for (FederationNamenodeContext namenode : namenodes) {
      if (!status.isShouldUseObserver()
          && (namenode.getState() == FederationNamenodeServiceState.OBSERVER)) {
        continue;
      }
      ConnectionContext connection = null;
      String nsId = namenode.getNameserviceId();
      String rpcAddress = namenode.getRpcAddress();
      try {
        connection = this.getConnection(ugi, nsId, rpcAddress, protocol);
        ProxyAndInfo<?> client = connection.getClient();
        final Object proxy = client.getProxy();

        ret = invoke(namenode, useObserver, 0, method, proxy, params);
        postProcessResult(method, status, namenode, nsId, client);
        return ret;
      } catch (IOException ioe) {
        ioes.put(namenode, ioe);
        handleInvokeMethodIOException(namenode, ioe, status, useObserver);
      } finally {
        if (connection != null) {
          connection.release();
        }
      }
    }
    if (this.rpcMonitor != null) {
      this.rpcMonitor.proxyOpComplete(false, null, null);
    }

    return handlerAllNamenodeFail(namenodes, method, ioes, params);
  }

  /**
   * All namenodes cannot successfully process the RPC request,
   * throw corresponding exceptions according to the exception type of each namenode.
   *
   * @param namenodes A prioritized list of namenodes within the same nameservice.
   * @param method Remote ClientProtocol method to invoke.
   * @param ioes The exception type of each namenode.
   * @param params Variable list of parameters matching the method.
   * @return null
   * @throws IOException Corresponding IOException according to the
   *                     exception type of each namenode.
   */
  protected Object handlerAllNamenodeFail(
      List<? extends FederationNamenodeContext> namenodes, Method method,
      Map<FederationNamenodeContext, IOException> ioes, Object[] params) throws IOException {
    // All namenodes were unavailable or in standby
    String msg = "No namenode available to invoke " + method.getName() + " " +
        Arrays.deepToString(params) + " in " + namenodes + " from " +
        router.getRouterId();
    LOG.error(msg);
    int exConnect = 0;
    for (Entry<FederationNamenodeContext, IOException> entry :
        ioes.entrySet()) {
      FederationNamenodeContext namenode = entry.getKey();
      String nnKey = namenode.getNamenodeKey();
      String addr = namenode.getRpcAddress();
      IOException ioe = entry.getValue();
      if (ioe instanceof StandbyException) {
        LOG.error("{} at {} is in Standby: {}",
            nnKey, addr, ioe.getMessage());
      } else if (isUnavailableException(ioe)) {
        exConnect++;
        LOG.error("{} at {} cannot be reached: {}",
            nnKey, addr, ioe.getMessage());
      } else {
        LOG.error("{} at {} error: \"{}\"", nnKey, addr, ioe.getMessage());
      }
    }
    if (exConnect == ioes.size()) {
      throw new ConnectException(msg);
    } else {
      throw new StandbyException(msg);
    }
  }

  /**
   * The RPC request is successfully processed by the NameNode, the NameNode status
   * in the router cache is updated according to the ExecutionStatus.
   *
   * @param method Remote method to invoke.
   * @param status Current execution status.
   * @param namenode The namenode that successfully processed this RPC request.
   * @param nsId Nameservice ID.
   * @param client Connection client.
   * @throws IOException If the state store cannot be accessed.
   */
  protected void postProcessResult(Method method, ExecutionStatus status,
      FederationNamenodeContext namenode, String nsId, ProxyAndInfo<?> client) throws IOException {
    if (status.isFailOver() &&
        FederationNamenodeServiceState.OBSERVER != namenode.getState()) {
      // Success on alternate server, update
      InetSocketAddress address = client.getAddress();
      namenodeResolver.updateActiveNamenode(nsId, address);
    }
    if (this.rpcMonitor != null) {
      this.rpcMonitor.proxyOpComplete(true, nsId, namenode.getState());
    }
    if (this.router.getRouterClientMetrics() != null) {
      this.router.getRouterClientMetrics().incInvokedMethod(method);
    }
  }

  /**
   * The RPC request to the NameNode throws an exception,
   * handle it according to the type of exception.
   *
   * @param namenode The namenode that processed this RPC request.
   * @param ioe The exception thrown by this RPC request.
   * @param status The current execution status.
   * @param useObserver Whether to use observer namenodes.
   * @throws IOException If it cannot invoke the method.
   */
  protected void handleInvokeMethodIOException(final FederationNamenodeContext namenode,
      IOException ioe, final ExecutionStatus status, boolean useObserver) throws IOException {
    String nsId = namenode.getNameserviceId();
    String rpcAddress = namenode.getRpcAddress();
    if (ioe instanceof ObserverRetryOnActiveException) {
      LOG.info("Encountered ObserverRetryOnActiveException from {}."
          + " Retry active namenode directly.", namenode);
      status.setShouldUseObserver(false);
    } else if (ioe instanceof StandbyException) {
      // Fail over indicated by retry policy and/or NN
      if (this.rpcMonitor != null) {
        this.rpcMonitor.proxyOpFailureStandby(nsId);
      }
      status.setFailOver(true);
    } else if (isUnavailableException(ioe)) {
      if (this.rpcMonitor != null) {
        this.rpcMonitor.proxyOpFailureCommunicate(nsId);
      }
      if (FederationNamenodeServiceState.OBSERVER == namenode.getState()) {
        namenodeResolver.updateUnavailableNamenode(nsId,
            NetUtils.createSocketAddr(namenode.getRpcAddress()));
      } else {
        status.setFailOver(true);
      }
    } else if (ioe instanceof RemoteException) {
      if (this.rpcMonitor != null) {
        this.rpcMonitor.proxyOpComplete(true, nsId, namenode.getState());
      }
      RemoteException re = (RemoteException) ioe;
      ioe = re.unwrapRemoteException();
      ioe = getCleanException(ioe);
      // RemoteException returned by NN
      throw ioe;
    } else if (ioe instanceof ConnectionNullException) {
      if (this.rpcMonitor != null) {
        this.rpcMonitor.proxyOpFailureCommunicate(nsId);
      }
      LOG.error("Get connection for {} {} error: {}", nsId, rpcAddress,
          ioe.getMessage());
      // Throw StandbyException so that client can retry
      StandbyException se = new StandbyException(ioe.getMessage());
      se.initCause(ioe);
      throw se;
    } else if (ioe instanceof NoNamenodesAvailableException) {
      IOException cause = (IOException) ioe.getCause();
      if (this.rpcMonitor != null) {
        this.rpcMonitor.proxyOpNoNamenodes(nsId);
      }
      LOG.error("Cannot get available namenode for {} {} error: {}",
          nsId, rpcAddress, ioe.getMessage());
      // Rotate cache so that client can retry the next namenode in the cache
      if (shouldRotateCache(cause)) {
        this.namenodeResolver.rotateCache(nsId, namenode, useObserver);
      }
      // Throw RetriableException so that client can retry
      throw new RetriableException(ioe);
    } else {
      // Other communication error, this is a failure
      // Communication retries are handled by the retry policy
      if (this.rpcMonitor != null) {
        this.rpcMonitor.proxyOpFailureCommunicate(nsId);
        this.rpcMonitor.proxyOpComplete(false, nsId, namenode.getState());
      }
      throw ioe;
    }
  }

  /**
   * For tracking some information about the actual client.
   * It adds trace info "clientIp:ip", "clientPort:port",
   * "clientId:id", "clientCallId:callId" and "realUser:userName"
   * in the caller context, removing the old values if they were
   * already present.
   *
   * @param ugi User group information.
   */
  protected void addClientInfoToCallerContext(UserGroupInformation ugi) {
    CallerContext ctx = CallerContext.getCurrent();
    String origContext = ctx == null ? null : ctx.getContext();
    byte[] origSignature = ctx == null ? null : ctx.getSignature();
    String realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = ugi.getRealUser().getUserName();
    }
    CallerContext.Builder builder =
        new CallerContext.Builder("", contextFieldSeparator)
            .append(CallerContext.CLIENT_IP_STR, Server.getRemoteAddress())
            .append(CallerContext.CLIENT_PORT_STR,
                Integer.toString(Server.getRemotePort()))
            .append(CallerContext.CLIENT_ID_STR,
                StringUtils.byteToHexString(Server.getClientId()))
            .append(CallerContext.CLIENT_CALL_ID_STR,
                Integer.toString(Server.getCallId()))
            .append(CallerContext.REAL_USER_STR, realUser)
            .setSignature(origSignature);
    // Append the original caller context
    if (origContext != null) {
      for (String part : origContext.split(contextFieldSeparator)) {
        String[] keyValue =
            part.split(CallerContext.Builder.KEY_VALUE_SEPARATOR, 2);
        if (keyValue.length == 2) {
          builder.appendIfAbsent(keyValue[0], keyValue[1]);
        } else if (keyValue.length == 1) {
          builder.append(keyValue[0]);
        }
      }
    }
    CallerContext.setCurrent(builder.build());
  }

  /**
   * Invokes a method on the designated object. Catches exceptions specific to
   * the invocation.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param namenode namenode context.
   * @param listObserverFirst Observer read case, observer NN will be ranked first.
   * @param retryCount Current retry times
   * @param method Method to invoke
   * @param obj Target object for the method
   * @param params Variable parameters
   * @return Response from the remote server
   * @throws IOException If error occurs.
   */
  protected Object invoke(
      FederationNamenodeContext namenode, Boolean listObserverFirst,
      int retryCount, final Method method,
      final Object obj, final Object... params) throws IOException {
    try {
      return method.invoke(obj, params);
    } catch (IllegalAccessException | IllegalArgumentException e) {
      LOG.error("Unexpected exception while proxying API", e);
      return null;
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      return handlerInvokeException(namenode, listObserverFirst,
          retryCount, method, obj, cause, params);
    }
  }

  /**
   * Handle the exception when an RPC request to the NameNode throws an exception.
   *
   * @param namenode namenode context.
   * @param listObserverFirst Observer read case, observer NN will be ranked first.
   * @param retryCount Current retry times
   * @param method Method to invoke
   * @param obj Target object for the method
   * @param e The exception thrown by the current invocation.
   * @param params Variable parameters
   * @return Response from the remote server
   * @throws IOException If error occurs.
   */
  protected Object handlerInvokeException(FederationNamenodeContext namenode,
      Boolean listObserverFirst, int retryCount, Method method, Object obj,
      Throwable e, Object[] params) throws IOException {
    String nsId = namenode.getNameserviceId();
    if (e instanceof IOException) {
      IOException ioe = (IOException) e;

      // Check if we should retry.
      RetryDecision decision = shouldRetry(ioe, retryCount, nsId, namenode, listObserverFirst);
      if (decision == RetryDecision.RETRY) {
        if (this.rpcMonitor != null) {
          this.rpcMonitor.proxyOpRetries();
        }

        // retry
        return invoke(namenode, listObserverFirst, ++retryCount, method, obj, params);
      } else if (decision == RetryDecision.FAILOVER_AND_RETRY) {
        // failover, invoker looks for standby exceptions for failover.
        if (ioe instanceof StandbyException) {
          throw ioe;
        } else if (isUnavailableException(ioe)) {
          throw ioe;
        } else {
          throw new StandbyException(ioe.getMessage());
        }
      } else {
        throw ioe;
      }
    } else {
      throw new IOException(e);
    }
  }


  /**
   * Check if the exception comes from an unavailable subcluster.
   * @param ioe IOException to check.
   * @return If the exception comes from an unavailable subcluster.
   */
  public static boolean isUnavailableException(IOException ioe) {
    if (ioe instanceof ConnectTimeoutException ||
        ioe instanceof EOFException ||
        ioe instanceof SocketException ||
        ioe instanceof StandbyException) {
      return true;
    }
    if (ioe instanceof RetriableException) {
      Throwable cause = ioe.getCause();
      if (cause instanceof NoNamenodesAvailableException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if the cluster of given nameservice id is available.
   *
   * @param nsId nameservice ID.
   * @param namenode namenode context.
   * @param listObserverFirst Observer read case, observer NN will be ranked first.
   * @return true if the cluster with given nameservice id is available.
   * @throws IOException if error occurs.
   */
  private boolean isClusterUnAvailable(
      String nsId, FederationNamenodeContext namenode,
      boolean listObserverFirst) throws IOException {
    // If the operation is an observer read
    // and the namenode that caused the exception is an observer,
    // false is returned so that the observer can be marked as unavailable,so other observers
    // or active namenode which is standby in the cache of the router can be retried.
    if (listObserverFirst && namenode.getState() == FederationNamenodeServiceState.OBSERVER) {
      return false;
    }
    List<? extends FederationNamenodeContext> nnState = this.namenodeResolver
        .getNamenodesForNameserviceId(nsId, listObserverFirst);
    if (nnState != null) {
      for (FederationNamenodeContext nnContext : nnState) {
        // Once we find one NN is in active state, we assume this
        // cluster is available.
        if (nnContext.getState() == FederationNamenodeServiceState.ACTIVE) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Get a clean copy of the exception. Sometimes the exceptions returned by the
   * server contain the full stack trace in the message.
   *
   * @param ioe Exception to clean up.
   * @return Copy of the original exception with a clean message.
   */
  protected static IOException getCleanException(IOException ioe) {
    IOException ret = null;

    String msg = ioe.getMessage();
    Throwable cause = ioe.getCause();
    StackTraceElement[] stackTrace = ioe.getStackTrace();

    // Clean the message by removing the stack trace
    int index = msg.indexOf("\n");
    if (index > 0) {
      String[] msgSplit = msg.split("\n");
      msg = msgSplit[0];

      // Parse stack trace from the message
      List<StackTraceElement> elements = new LinkedList<>();
      for (int i=1; i<msgSplit.length; i++) {
        String line = msgSplit[i];
        Matcher matcher = STACK_TRACE_PATTERN.matcher(line);
        if (matcher.find()) {
          String declaringClass = matcher.group(1);
          String methodName = matcher.group(2);
          String fileName = matcher.group(3);
          int lineNumber = Integer.parseInt(matcher.group(4));
          StackTraceElement element = new StackTraceElement(
              declaringClass, methodName, fileName, lineNumber);
          elements.add(element);
        }
      }
      stackTrace = elements.toArray(new StackTraceElement[elements.size()]);
    }

    // Create the new output exception
    if (ioe instanceof RemoteException) {
      RemoteException re = (RemoteException)ioe;
      ret = new RemoteException(re.getClassName(), msg);
    } else {
      // Try the simple constructor and initialize the fields
      Class<? extends IOException> ioeClass = ioe.getClass();
      try {
        Constructor<? extends IOException> constructor =
            ioeClass.getDeclaredConstructor(String.class);
        ret = constructor.newInstance(msg);
      } catch (ReflectiveOperationException e) {
        // If there are errors, just use the input one
        LOG.error("Could not create exception {}", ioeClass.getSimpleName(), e);
        ret = ioe;
      }
    }
    if (ret != null) {
      ret.initCause(cause);
      ret.setStackTrace(stackTrace);
    }

    return ret;
  }

  /**
   * Try to get the remote location whose bpId is same with the input bpId from the input locations.
   * @param locations the input RemoteLocations.
   * @param bpId the input bpId.
   * @return the remote location whose bpId is same with the input.
   * @throws IOException
   */
  private RemoteLocation getLocationWithBPID(List<RemoteLocation> locations, String bpId)
      throws IOException {
    String nsId = getNameserviceForBlockPoolId(bpId);
    for (RemoteLocation l : locations) {
      if (l.getNameserviceId().equals(nsId)) {
        return l;
      }
    }

    LOG.debug("Can't find remote location for the {} from {}", bpId, locations);
    return new RemoteLocation(nsId, "/", "/");
  }

  /**
   * Invokes a ClientProtocol method. Determines the target nameservice via a
   * provided block.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param block Block used to determine appropriate nameservice.
   * @param method The remote method and parameters to invoke.
   * @param locations The remote locations will be used.
   * @param clazz Class for the return type.
   * @param <T> The type of the remote method return.
   * @return The result of invoking the method.
   * @throws IOException If the invoke generated an error.
   */
  public <T> T invokeSingle(final ExtendedBlock block, RemoteMethod method,
      final List<RemoteLocation> locations, Class<T> clazz) throws IOException {
    RemoteLocation location = getLocationWithBPID(locations, block.getBlockPoolId());
    return invokeSingle(location, method, clazz);
  }

  /**
   * Invokes a ClientProtocol method. Determines the target nameservice using
   * the block pool id.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param bpId Block pool identifier.
   * @param method The remote method and parameters to invoke.
   * @return The result of invoking the method.
   * @throws IOException If the invoke generated an error.
   */
  public Object invokeSingleBlockPool(final String bpId, RemoteMethod method)
      throws IOException {
    String nsId = getNameserviceForBlockPoolId(bpId);
    return invokeSingle(nsId, method);
  }

  /**
   * Invokes a ClientProtocol method against the specified namespace.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param nsId Target namespace for the method.
   * @param method The remote method and parameters to invoke.
   * @return The result of invoking the method.
   * @throws IOException If the invoke generated an error.
   */
  public Object invokeSingle(final String nsId, RemoteMethod method)
      throws IOException {
    UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
    acquirePermit(nsId, ugi, method, controller);
    try {
      boolean isObserverRead = isObserverReadEligible(nsId, method.getMethod());
      List<? extends FederationNamenodeContext> nns = getOrderedNamenodes(nsId, isObserverRead);
      RemoteLocationContext loc = new RemoteLocation(nsId, "/", "/");
      Class<?> proto = method.getProtocol();
      Method m = method.getMethod();
      Object[] params = method.getParams(loc);
      return invokeMethod(ugi, nns, isObserverRead, proto, m, params);
    } finally {
      releasePermit(nsId, ugi, method, controller);
    }
  }

  /**
   * Invokes a remote method against the specified namespace.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param <T> The type of the remote method return.
   * @param nsId Target namespace for the method.
   * @param method The remote method and parameters to invoke.
   * @param clazz Class for the return type.
   * @return The result of invoking the method.
   * @throws IOException If the invoke generated an error.
   */
  public <T> T invokeSingle(final String nsId, RemoteMethod method,
      Class<T> clazz) throws IOException {
    @SuppressWarnings("unchecked")
    T ret = (T)invokeSingle(nsId, method);
    return ret;
  }

  /**
   * Invokes a remote method against the specified extendedBlock.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param <T> The type of the remote method return.
   * @param extendedBlock Target extendedBlock for the method.
   * @param method The remote method and parameters to invoke.
   * @param clazz Class for the return type.
   * @return The result of invoking the method.
   * @throws IOException If the invoke generated an error.
   */
  public <T> T invokeSingle(final ExtendedBlock extendedBlock,
      RemoteMethod method, Class<T> clazz) throws IOException {
    String nsId = getNameserviceForBlockPoolId(extendedBlock.getBlockPoolId());
    @SuppressWarnings("unchecked")
    T ret = (T)invokeSingle(nsId, method);
    return ret;
  }

  /**
   * Invokes a single proxy call for a single location.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param location RemoteLocation to invoke.
   * @param remoteMethod The remote method and parameters to invoke.
   * @param clazz Class for the return type.
   * @param <T> The type of the remote method return.
   * @return The result of invoking the method if successful.
   * @throws IOException If the invoke generated an error.
   */
  public <T> T invokeSingle(final RemoteLocationContext location,
      RemoteMethod remoteMethod, Class<T> clazz) throws IOException {
    List<RemoteLocationContext> locations = Collections.singletonList(location);
    @SuppressWarnings("unchecked")
    T ret = (T)invokeSequential(locations, remoteMethod);
    return ret;
  }

  /**
   * Invokes sequential proxy calls to different locations. Continues to invoke
   * calls until a call returns without throwing a remote exception.
   *
   * @param locations List of locations/nameservices to call concurrently.
   * @param remoteMethod The remote method and parameters to invoke.
   * @param <T> The type of the remote method return.
   * @return The result of the first successful call, or if no calls are
   * successful, the result of the last RPC call executed.
   * @throws IOException if the success condition is not met and one of the RPC
   * calls generated a remote exception.
   */
  public <T> T invokeSequential(
      final List<? extends RemoteLocationContext> locations,
      final RemoteMethod remoteMethod) throws IOException {
    return invokeSequential(locations, remoteMethod, null, null);
  }

  /**
   * Invokes sequential proxy calls to different locations. Continues to invoke
   * calls until the success condition is met, or until all locations have been
   * attempted.
   *
   * The success condition may be specified by:
   * <ul>
   * <li>An expected result class
   * <li>An expected result value
   * </ul>
   *
   * If no expected result class/values are specified, the success condition is
   * a call that does not throw a remote exception.
   *
   * @param <T> The type of the remote method return.
   * @param locations List of locations/nameservices to call concurrently.
   * @param remoteMethod The remote method and parameters to invoke.
   * @param expectedResultClass In order to be considered a positive result, the
   *          return type must be of this class.
   * @param expectedResultValue In order to be considered a positive result, the
   *          return value must equal the value of this object.
   * @return The result of the first successful call, or if no calls are
   *         successful, the result of the first RPC call executed.
   * @throws IOException if the success condition is not met, return the first
   *                     remote exception generated.
   */
  public <T> T invokeSequential(
      final List<? extends RemoteLocationContext> locations,
      final RemoteMethod remoteMethod, Class<T> expectedResultClass,
      Object expectedResultValue) throws IOException {
    return (T) invokeSequential(remoteMethod, locations, expectedResultClass,
        expectedResultValue).getResult();
  }

  /**
   * Invokes sequential proxy calls to different locations. Continues to invoke
   * calls until the success condition is met, or until all locations have been
   * attempted.
   *
   * The success condition may be specified by:
   * <ul>
   * <li>An expected result class
   * <li>An expected result value
   * </ul>
   *
   * If no expected result class/values are specified, the success condition is
   * a call that does not throw a remote exception.
   *
   * This returns RemoteResult, which contains the invoked location as well
   * as the result.
   *
   * @param <R> The type of the remote location.
   * @param <T> The type of the remote method return.
   * @param remoteMethod The remote method and parameters to invoke.
   * @param locations List of locations/nameservices to call concurrently.
   * @param expectedResultClass In order to be considered a positive result, the
   *          return type must be of this class.
   * @param expectedResultValue In order to be considered a positive result, the
   *          return value must equal the value of this object.
   * @return The result of the first successful call, or if no calls are
   *         successful, the result of the first RPC call executed, along with
   *         the invoked location in form of RemoteResult.
   * @throws IOException if the success condition is not met, return the first
   *                     remote exception generated.
   */
  public <R extends RemoteLocationContext, T> RemoteResult invokeSequential(
      final RemoteMethod remoteMethod, final List<R> locations,
      Class<T> expectedResultClass, Object expectedResultValue)
      throws IOException {

    RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = remoteMethod.getMethod();
    List<IOException> thrownExceptions = new ArrayList<>();
    Object firstResult = null;
    // Invoke in priority order
    for (final RemoteLocationContext loc : locations) {
      String ns = loc.getNameserviceId();
      boolean isObserverRead = isObserverReadEligible(ns, m);
      List<? extends FederationNamenodeContext> namenodes =
          getOrderedNamenodes(ns, isObserverRead);
      acquirePermit(ns, ugi, remoteMethod, controller);
      try {
        Class<?> proto = remoteMethod.getProtocol();
        Object[] params = remoteMethod.getParams(loc);
        Object result = invokeMethod(
            ugi, namenodes, isObserverRead, proto, m, params);
        // Check if the result is what we expected
        if (isExpectedClass(expectedResultClass, result) &&
            isExpectedValue(expectedResultValue, result)) {
          // Valid result, stop here
          @SuppressWarnings("unchecked") R location = (R) loc;
          @SuppressWarnings("unchecked") T ret = (T) result;
          return new RemoteResult<>(location, ret);
        }
        if (firstResult == null) {
          firstResult = result;
        }
      } catch (IOException ioe) {
        // Localize the exception

        ioe = processException(ioe, loc);

        // Record it and move on
        thrownExceptions.add(ioe);
      } catch (Exception e) {
        // Unusual error, ClientProtocol calls always use IOException (or
        // RemoteException). Re-wrap in IOException for compatibility with
        // ClientProtocol.
        LOG.error("Unexpected exception {} proxying {} to {}",
            e.getClass(), m.getName(), ns, e);
        IOException ioe = new IOException(
            "Unexpected exception proxying API " + e.getMessage(), e);
        thrownExceptions.add(ioe);
      } finally {
        releasePermit(ns, ugi, remoteMethod, controller);
      }
    }

    if (!thrownExceptions.isEmpty()) {
      // An unavailable subcluster may be the actual cause
      // We cannot surface other exceptions (e.g., FileNotFoundException)
      for (int i = 0; i < thrownExceptions.size(); i++) {
        IOException ioe = thrownExceptions.get(i);
        if (isUnavailableException(ioe)) {
          throw ioe;
        }
      }

      // re-throw the first exception thrown for compatibility
      throw thrownExceptions.get(0);
    }
    // Return the first result, whether it is the value or not
    @SuppressWarnings("unchecked") T ret = (T) firstResult;
    return new RemoteResult<>(locations.get(0), ret);
  }

  /**
   * Exception messages might contain local subcluster paths. This method
   * generates a new exception with the proper message.
   * @param ioe Original IOException.
   * @param loc Location we are processing.
   * @return Exception processed for federation.
   */
  protected IOException processException(
      IOException ioe, RemoteLocationContext loc) {

    if (ioe instanceof RemoteException) {
      RemoteException re = (RemoteException)ioe;
      String newMsg = processExceptionMsg(
          re.getMessage(), loc.getDest(), loc.getSrc());
      RemoteException newException =
          new RemoteException(re.getClassName(), newMsg);
      newException.setStackTrace(ioe.getStackTrace());
      return newException;
    }

    if (ioe instanceof FileNotFoundException) {
      String newMsg = processExceptionMsg(
          ioe.getMessage(), loc.getDest(), loc.getSrc());
      FileNotFoundException newException = new FileNotFoundException(newMsg);
      newException.setStackTrace(ioe.getStackTrace());
      return newException;
    }

    if (ioe instanceof SnapshotException) {
      String newMsg = processExceptionMsg(
          ioe.getMessage(), loc.getDest(), loc.getSrc());
      SnapshotException newException = new SnapshotException(newMsg);
      newException.setStackTrace(ioe.getStackTrace());
      return newException;
    }

    return ioe;
  }

  /**
   * Process a subcluster message and make it federated.
   * @param msg Original exception message.
   * @param dst Path in federation.
   * @param src Path in the subcluster.
   * @return Message processed for federation.
   */
  @VisibleForTesting
  static String processExceptionMsg(
      final String msg, final String dst, final String src) {
    if (dst.equals(src) || !dst.startsWith("/") || !src.startsWith("/")) {
      return msg;
    }

    String newMsg = msg.replaceFirst(dst, src);
    int minLen = Math.min(dst.length(), src.length());
    for (int i = 0; newMsg.equals(msg) && i < minLen; i++) {
      // Check if we can replace sub folders
      String dst1 = dst.substring(0, dst.length() - 1 - i);
      String src1 = src.substring(0, src.length() - 1 - i);
      newMsg = msg.replaceFirst(dst1, src1);
    }

    return newMsg;
  }

  /**
   * Checks if a result matches the required result class.
   *
   * @param expectedClass Required result class, null to skip the check.
   * @param clazz The result to check.
   * @return True if the result is an instance of the required class or if the
   *         expected class is null.
   */
  protected static boolean isExpectedClass(Class<?> expectedClass, Object clazz) {
    if (expectedClass == null) {
      return true;
    } else if (clazz == null) {
      return false;
    } else {
      return expectedClass.isInstance(clazz);
    }
  }

  /**
   * Checks if a result matches the expected value.
   *
   * @param expectedValue The expected value, null to skip the check.
   * @param value The result to check.
   * @return True if the result is equals to the expected value or if the
   *         expected value is null.
   */
  protected static boolean isExpectedValue(Object expectedValue, Object value) {
    if (expectedValue == null) {
      return true;
    } else if (value == null) {
      return false;
    } else {
      return value.equals(expectedValue);
    }
  }

  /**
   * Invoke method in all locations and return success if any succeeds.
   *
   * @param <T> The type of the remote location.
   * @param locations List of remote locations to call concurrently.
   * @param method The remote method and parameters to invoke.
   * @return If the call succeeds in any location.
   * @throws IOException If any of the calls return an exception.
   */
  public <T extends RemoteLocationContext> boolean invokeAll(
      final Collection<T> locations, final RemoteMethod method)
      throws IOException {
    Map<T, Boolean> results =
        invokeConcurrent(locations, method, false, false, Boolean.class);
    return results.containsValue(true);
  }

  /**
   * Invoke multiple concurrent proxy calls to different clients. Returns an
   * array of results.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param <T> The type of the remote location.
   * @param <R> The type of the remote method return.
   * @param locations List of remote locations to call concurrently.
   * @param method The remote method and parameters to invoke.
   * @throws IOException If all the calls throw an exception.
   */
  public <T extends RemoteLocationContext, R> void invokeConcurrent(
      final Collection<T> locations, final RemoteMethod method)
          throws IOException {
    invokeConcurrent(locations, method, void.class);
  }

  /**
   * Invoke multiple concurrent proxy calls to different clients. Returns an
   * array of results.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param locations List of remote locations to call concurrently.
   * @param method The remote method and parameters to invoke.
   * @param clazz Type of the remote return type.
   * @param <T> The type of the remote location.
   * @param <R> The type of the remote method return.
   * @return Result of invoking the method per subcluster: nsId to result.
   * @throws IOException If all the calls throw an exception.
   */
  public <T extends RemoteLocationContext, R> Map<T, R> invokeConcurrent(
      final Collection<T> locations, final RemoteMethod method, Class<R> clazz)
          throws IOException {
    return invokeConcurrent(locations, method, false, false, clazz);
  }

  /**
   * Invoke multiple concurrent proxy calls to different clients. Returns an
   * array of results.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param <T> The type of the remote location.
   * @param <R> The type of the remote method return.
   * @param locations List of remote locations to call concurrently.
   * @param method The remote method and parameters to invoke.
   * @param requireResponse If true an exception will be thrown if all calls do
   *          not complete. If false exceptions are ignored and all data results
   *          successfully received are returned.
   * @param standby If the requests should go to the standby namenodes too.
   * @throws IOException If all the calls throw an exception.
   */
  public <T extends RemoteLocationContext, R> void invokeConcurrent(
      final Collection<T> locations, final RemoteMethod method,
      boolean requireResponse, boolean standby) throws IOException {
    invokeConcurrent(locations, method, requireResponse, standby, void.class);
  }

  /**
   * Invokes multiple concurrent proxy calls to different clients. Returns an
   * array of results.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param <T> The type of the remote location.
   * @param <R> The type of the remote method return.
   * @param locations List of remote locations to call concurrently.
   * @param method The remote method and parameters to invoke.
   * @param requireResponse If true an exception will be thrown if all calls do
   *          not complete. If false exceptions are ignored and all data results
   *          successfully received are returned.
   * @param standby If the requests should go to the standby namenodes too.
   * @param clazz Type of the remote return type.
   * @return Result of invoking the method per subcluster: nsId to result.
   * @throws IOException If requiredResponse=true and any of the calls throw an
   *           exception.
   */
  public <T extends RemoteLocationContext, R> Map<T, R> invokeConcurrent(
      final Collection<T> locations, final RemoteMethod method,
      boolean requireResponse, boolean standby, Class<R> clazz)
          throws IOException {
    return invokeConcurrent(
        locations, method, requireResponse, standby, -1, clazz);
  }

  /**
   * Invokes multiple concurrent proxy calls to different clients. Returns an
   * array of results.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param <T> The type of the remote location.
   * @param <R> The type of the remote method return.
   * @param locations List of remote locations to call concurrently.
   * @param method The remote method and parameters to invoke.
   * @param requireResponse If true an exception will be thrown if all calls do
   *          not complete. If false exceptions are ignored and all data results
   *          successfully received are returned.
   * @param standby If the requests should go to the standby namenodes too.
   * @param timeOutMs Timeout for each individual call.
   * @param clazz Type of the remote return type.
   * @return Result of invoking the method per subcluster: nsId to result.
   * @throws IOException If requiredResponse=true and any of the calls throw an
   *           exception.
   */
  public <T extends RemoteLocationContext, R> Map<T, R> invokeConcurrent(
      final Collection<T> locations, final RemoteMethod method,
      boolean requireResponse, boolean standby, long timeOutMs, Class<R> clazz)
          throws IOException {
    final List<RemoteResult<T, R>> results = invokeConcurrent(
        locations, method, standby, timeOutMs, clazz);
    return postProcessResult(requireResponse, results);
  }

  /**
   * Post-process the results returned by
   * {@link RouterRpcClient#invokeConcurrent(Collection, RemoteMethod, boolean, long, Class)}.
   *
   * @param requireResponse If true an exception will be thrown if all calls do
   *          not complete. If false exceptions are ignored and all data results
   *          successfully received are returned.
   * @param results Result of invoking the method per subcluster (list of results),
   *                This includes the exception for each remote location.
   * @return Result of invoking the method per subcluster: nsId to result.
   * @param <T> The type of the remote location.
   * @param <R> The type of the remote method return.
   * @throws IOException If requiredResponse=true and any of the calls throw an
   *                     exception.
   */
  protected static <T extends RemoteLocationContext, R> Map<T, R> postProcessResult(
      boolean requireResponse, List<RemoteResult<T, R>> results) throws IOException {
    // Go over the results and exceptions
    final Map<T, R> ret = new TreeMap<>();
    final List<IOException> thrownExceptions = new ArrayList<>();
    IOException firstUnavailableException = null;
    for (final RemoteResult<T, R> result : results) {
      if (result.hasException()) {
        IOException ioe = result.getException();
        thrownExceptions.add(ioe);
        // Track unavailable exceptions to throw them first
        if (isUnavailableException(ioe)) {
          firstUnavailableException = ioe;
        }
      }
      if (result.hasResult()) {
        ret.put(result.getLocation(), result.getResult());
      }
    }

    // Throw exceptions if needed
    if (!thrownExceptions.isEmpty()) {
      // Throw if response from all servers required or no results
      if (requireResponse || ret.isEmpty()) {
        // Throw unavailable exceptions first
        if (firstUnavailableException != null) {
          throw firstUnavailableException;
        } else {
          throw thrownExceptions.get(0);
        }
      }
    }

    return ret;
  }

  /**
   * Invokes multiple concurrent proxy calls to different clients. Returns an
   * array of results.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param <T> The type of the remote location.
   * @param <R> The type of the remote method return
   * @param locations List of remote locations to call concurrently.
   * @param method The remote method and parameters to invoke.
   * @param standby If the requests should go to the standby namenodes too.
   * @param timeOutMs Timeout for each individual call.
   * @param clazz Type of the remote return type.
   * @return Result of invoking the method per subcluster (list of results).
   *         This includes the exception for each remote location.
   * @throws IOException If there are errors invoking the method.
   */
  @SuppressWarnings("unchecked")
  public <T extends RemoteLocationContext, R> List<RemoteResult<T, R>>
      invokeConcurrent(final Collection<T> locations,
          final RemoteMethod method, boolean standby, long timeOutMs,
          Class<R> clazz) throws IOException {

    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = method.getMethod();

    if (locations.isEmpty()) {
      throw new IOException("No remote locations available");
    } else if (locations.size() == 1 && timeOutMs <= 0) {
      // Shortcut, just one call
      return invokeSingle(locations.iterator().next(), method);
    }
    RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
    acquirePermit(CONCURRENT_NS, ugi, method, controller);

    List<T> orderedLocations = new ArrayList<>();
    List<Callable<Object>> callables = new ArrayList<>();
    // transfer originCall & callerContext to worker threads of executor.
    final Call originCall = Server.getCurCall().get();
    final CallerContext originContext = CallerContext.getCurrent();
    for (final T location : locations) {
      String nsId = location.getNameserviceId();
      boolean isObserverRead = isObserverReadEligible(nsId, m);
      final List<? extends FederationNamenodeContext> namenodes =
          getOrderedNamenodes(nsId, isObserverRead);
      final Class<?> proto = method.getProtocol();
      final Object[] paramList = method.getParams(location);
      if (standby) {
        // Call the objectGetter to all NNs (including standby)
        for (final FederationNamenodeContext nn : namenodes) {
          String nnId = nn.getNamenodeId();
          final List<FederationNamenodeContext> nnList =
              Collections.singletonList(nn);
          T nnLocation = location;
          if (location instanceof RemoteLocation) {
            nnLocation = (T)new RemoteLocation(nsId, nnId, location.getDest());
          }
          orderedLocations.add(nnLocation);
          callables.add(
              () -> {
                transferThreadLocalContext(originCall, originContext);
                return invokeMethod(
                    ugi, nnList, isObserverRead, proto, m, paramList);
              });
        }
      } else {
        // Call the objectGetter in order of nameservices in the NS list
        orderedLocations.add(location);
        callables.add(
            () -> {
              transferThreadLocalContext(originCall, originContext);
              return invokeMethod(
                  ugi, namenodes, isObserverRead, proto, m, paramList);
            });
      }
    }

    if (rpcMonitor != null) {
      rpcMonitor.proxyOp();
    }
    if (this.router.getRouterClientMetrics() != null) {
      this.router.getRouterClientMetrics().incInvokedConcurrent(m);
    }

    return getRemoteResults(method, timeOutMs, controller, orderedLocations, callables);
  }

  /**
   * Invokes multiple concurrent proxy calls to different clients. Returns an
   * array of results.
   *
   * @param <T> The type of the remote location.
   * @param <R> The type of the remote method return.
   * @param method The remote method and parameters to invoke.
   * @param timeOutMs Timeout for each individual call.
   * @param controller Fairness manager to control handlers assigned per NS.
   * @param orderedLocations List of remote locations to call concurrently.
   * @param callables Invoke method for each NameNode.
   * @return Result of invoking the method per subcluster (list of results),
   *         This includes the exception for each remote location.
   * @throws IOException If there are errors invoking the method.
   */
  protected  <T extends RemoteLocationContext, R> List<RemoteResult<T, R>> getRemoteResults(
      RemoteMethod method, long timeOutMs, RouterRpcFairnessPolicyController controller,
      List<T> orderedLocations, List<Callable<Object>> callables) throws IOException {
    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = method.getMethod();
    try {
      List<Future<Object>> futures = null;
      if (timeOutMs > 0) {
        futures = executorService.invokeAll(
            callables, timeOutMs, TimeUnit.MILLISECONDS);
      } else {
        futures = executorService.invokeAll(callables);
      }
      return processFutures(method, m, orderedLocations, futures);
    } catch (RejectedExecutionException e) {
      if (rpcMonitor != null) {
        rpcMonitor.proxyOpFailureClientOverloaded();
      }
      int active = executorService.getActiveCount();
      int total = executorService.getMaximumPoolSize();
      String msg = "Not enough client threads " + active + "/" + total;
      LOG.error(msg);
      throw new StandbyException(
          "Router " + router.getRouterId() + " is overloaded: " + msg);
    } catch (InterruptedException ex) {
      LOG.error("Unexpected error while invoking API: {}", ex.getMessage());
      throw new IOException(
          "Unexpected error while invoking API " + ex.getMessage(), ex);
    } finally {
      releasePermit(CONCURRENT_NS, ugi, method, controller);
    }
  }

  /**
   * Handle all futures during the invokeConcurrent call process.
   *
   * @param <T> The type of the remote location.
   * @param <R> The type of the remote method return.
   * @param method The remote method and parameters to invoke.
   * @param m The method to invoke.
   * @param orderedLocations List of remote locations to call concurrently.
   * @param futures all futures during the invokeConcurrent call process.
   * @return Result of invoking the method per subcluster (list of results),
   *         This includes the exception for each remote location.
   * @throws InterruptedException if the current thread was interrupted while waiting.
   */
  protected <T extends RemoteLocationContext, R> List<RemoteResult<T, R>> processFutures(
      RemoteMethod method, Method m, final List<T> orderedLocations,
      final List<Future<Object>> futures) throws InterruptedException{
    List<RemoteResult<T, R>> results = new ArrayList<>();
    for (int i = 0; i< futures.size(); i++) {
      T location = orderedLocations.get(i);
      try {
        Future<Object> future = futures.get(i);
        R result = (R) future.get();
        results.add(new RemoteResult<>(location, result));
      } catch (CancellationException ce) {
        T loc = orderedLocations.get(i);
        String msg = "Invocation to \"" + loc + "\" for \""
            + method.getMethodName() + "\" timed out";
        LOG.error(msg);
        IOException ioe = new SubClusterTimeoutException(msg);
        results.add(new RemoteResult<>(location, ioe));
      } catch (ExecutionException ex) {
        Throwable cause = ex.getCause();
        LOG.debug("Cannot execute {} in {}: {}",
            m.getName(), location, cause.getMessage());

        // Convert into IOException if needed
        IOException ioe = null;
        if (cause instanceof IOException) {
          ioe = (IOException) cause;
        } else {
          ioe = new IOException("Unhandled exception while proxying API " +
              m.getName() + ": " + cause.getMessage(), cause);
        }

        // Store the exceptions
        results.add(new RemoteResult<>(location, ioe));
      }
    }
    if (rpcMonitor != null) {
      rpcMonitor.proxyOpComplete(true, CONCURRENT, null);
    }
    return results;
  }

  /**
   * Invokes a ClientProtocol method against the specified namespace.
   * <p>
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param <T> The type of the remote location.
   * @param <R> The type of the remote method return.
   * @param location RemoteLocation to invoke.
   * @param method The remote method and parameters to invoke.
   * @return Result of invoking the method per subcluster (list of results),
   *         This includes the exception for each remote location.
   * @throws IOException If there are errors invoking the method.
   */
  public <T extends RemoteLocationContext, R> List<RemoteResult<T, R>> invokeSingle(
      T location, RemoteMethod method) throws IOException {
    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = method.getMethod();
    String ns = location.getNameserviceId();
    boolean isObserverRead = isObserverReadEligible(ns, m);
    final List<? extends FederationNamenodeContext> namenodes =
        getOrderedNamenodes(ns, isObserverRead);
    RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
    acquirePermit(ns, ugi, method, controller);
    try {
      Class<?> proto = method.getProtocol();
      Object[] paramList = method.getParams(location);
      R result = (R) invokeMethod(
          ugi, namenodes, isObserverRead, proto, m, paramList);
      RemoteResult<T, R> remoteResult = new RemoteResult<>(location, result);
      return Collections.singletonList(remoteResult);
    } catch (IOException ioe) {
      // Localize the exception
      throw processException(ioe, location);
    } finally {
      releasePermit(ns, ugi, method, controller);
    }
  }

  /**
   * Transfer origin thread local context which is necessary to current
   * worker thread when invoking method concurrently by executor service.
   *
   * @param originCall origin Call required for getting remote client ip.
   * @param originContext origin CallerContext which should be transferred
   *                      to server side.
   */
  protected void transferThreadLocalContext(
      final Call originCall, final CallerContext originContext) {
    Server.getCurCall().set(originCall);
    CallerContext.setCurrent(originContext);
  }

  /**
   * Get a prioritized list of NNs that share the same block pool ID (in the
   * same namespace). NNs that are reported as ACTIVE will be first in the list.
   *
   * @param bpId The blockpool ID for the namespace.
   * @return A prioritized list of NNs to use for communication.
   * @throws IOException If a NN cannot be located for the block pool ID.
   */
  private List<? extends FederationNamenodeContext> getNamenodesForBlockPoolId(
      final String bpId) throws IOException {

    List<? extends FederationNamenodeContext> namenodes =
        namenodeResolver.getNamenodesForBlockPoolId(bpId);

    if (namenodes == null || namenodes.isEmpty()) {
      throw new IOException("Cannot locate a registered namenode for " + bpId +
          " from " + router.getRouterId());
    }
    return namenodes;
  }

  /**
   * Get the nameservice identifier for a block pool.
   *
   * @param bpId Identifier of the block pool.
   * @return Nameservice identifier.
   * @throws IOException If a NN cannot be located for the block pool ID.
   */
  private String getNameserviceForBlockPoolId(final String bpId)
      throws IOException {
    List<? extends FederationNamenodeContext> namenodes =
        getNamenodesForBlockPoolId(bpId);
    FederationNamenodeContext namenode = namenodes.get(0);
    return namenode.getNameserviceId();
  }

  /**
   * Acquire permit to continue processing the request for specific nsId.
   *
   * @param nsId Identifier of the block pool.
   * @param ugi UserGroupIdentifier associated with the user.
   * @param m Remote method that needs to be invoked.
   * @param controller fairness policy controller to acquire permit from
   * @throws IOException If permit could not be acquired for the nsId.
   */
  protected void acquirePermit(final String nsId, final UserGroupInformation ugi,
      final RemoteMethod m, RouterRpcFairnessPolicyController controller)
      throws IOException {
    if (controller != null) {
      if (!controller.acquirePermit(nsId)) {
        // Throw StandByException,
        // Clients could fail over and try another router.
        if (rpcMonitor != null) {
          rpcMonitor.proxyOpPermitRejected(nsId);
        }
        incrRejectedPermitForNs(nsId);
        LOG.debug("Permit denied for ugi: {} for method: {}",
            ugi, m.getMethodName());
        String msg =
            "Router " + router.getRouterId() +
                " is overloaded for NS: " + nsId;
        throw new StandbyException(msg);
      }
      if (rpcMonitor != null) {
        rpcMonitor.proxyOpPermitAccepted(nsId);
      }
      incrAcceptedPermitForNs(nsId);
    }
  }

  /**
   * Release permit for specific nsId after processing against downstream
   * nsId is completed.
   *  @param nsId Identifier of the block pool.
   * @param ugi UserGroupIdentifier associated with the user.
   * @param m Remote method that needs to be invoked.
   * @param controller fairness policy controller to release permit from
   */
  protected void releasePermit(final String nsId, final UserGroupInformation ugi,
      final RemoteMethod m, RouterRpcFairnessPolicyController controller) {
    if (controller != null) {
      controller.releasePermit(nsId);
      LOG.trace("Permit released for ugi: {} for method: {}", ugi,
          m.getMethodName());
    }
  }

  public RouterRpcFairnessPolicyController
      getRouterRpcFairnessPolicyController() {
    return routerRpcFairnessPolicyController;
  }

  private void incrRejectedPermitForNs(String ns) {
    rejectedPermitsPerNs.computeIfAbsent(ns, k -> new LongAdder()).increment();
  }

  public Long getRejectedPermitForNs(String ns) {
    return rejectedPermitsPerNs.containsKey(ns) ?
        rejectedPermitsPerNs.get(ns).longValue() : 0L;
  }

  private void incrAcceptedPermitForNs(String ns) {
    acceptedPermitsPerNs.computeIfAbsent(ns, k -> new LongAdder()).increment();
  }

  public Long getAcceptedPermitForNs(String ns) {
    return acceptedPermitsPerNs.containsKey(ns) ?
        acceptedPermitsPerNs.get(ns).longValue() : 0L;
  }

  /**
   * Refreshes/changes the fairness policy controller implementation if possible
   * and returns the controller class name.
   * @param conf Configuration
   * @return New controller class name if successfully refreshed, else old controller class name
   */
  public synchronized String refreshFairnessPolicyController(Configuration conf) {
    RouterRpcFairnessPolicyController newController;
    try {
      newController = FederationUtil.newFairnessPolicyController(conf);
    } catch (RuntimeException e) {
      LOG.error("Failed to create router fairness policy controller", e);
      return getCurrentFairnessPolicyControllerClassName();
    }

    if (newController != null) {
      if (routerRpcFairnessPolicyController != null) {
        routerRpcFairnessPolicyController.shutdown();
      }
      routerRpcFairnessPolicyController = newController;
    }
    return getCurrentFairnessPolicyControllerClassName();
  }

  private String getCurrentFairnessPolicyControllerClassName() {
    if (routerRpcFairnessPolicyController != null) {
      return routerRpcFairnessPolicyController.getClass().getCanonicalName();
    }
    return null;
  }

  /**
   * Get a prioritized list of NNs that share the same nameservice ID (in the
   * same namespace).
   * In observer read case, OBSERVER NNs will be first in the list.
   * Otherwise, ACTIVE NNs will be first in the list.
   *
   * @param nsId The nameservice ID for the namespace.
   * @param isObserverRead Read on observer namenode.
   * @return A prioritized list of NNs to use for communication.
   * @throws IOException If a NN cannot be located for the nameservice ID.
   */
  protected List<? extends FederationNamenodeContext> getOrderedNamenodes(String nsId,
      boolean isObserverRead) throws IOException {
    final List<? extends FederationNamenodeContext> namenodes;

    boolean listObserverNamenodesFirst = isObserverRead
        && isNamespaceStateIdFresh(nsId)
        && (RouterStateIdContext.getClientStateIdFromCurrentCall(nsId) > Long.MIN_VALUE);
    namenodes = namenodeResolver.getNamenodesForNameserviceId(nsId, listObserverNamenodesFirst);
    if (!listObserverNamenodesFirst) {
      // Refresh time of last call to active NameNode.
      getTimeOfLastCallToActive(nsId).accumulate(Time.monotonicNow());
    }

    if (namenodes == null || namenodes.isEmpty()) {
      throw new IOException("Cannot locate a registered namenode for " + nsId +
          " from " + router.getRouterId());
    }
    return namenodes;
  }

  protected boolean isObserverReadEligible(String nsId, Method method) {
    return isReadCall(method) && isNamespaceObserverReadEligible(nsId);
  }

  /**
   * Check if a namespace is eligible for observer reads.
   * @param nsId namespaceID
   * @return whether the 'namespace' has observer reads enabled.
   */
  boolean isNamespaceObserverReadEligible(String nsId) {
    return observerReadEnabledDefault != observerReadEnabledOverrides.contains(nsId);
  }

  /**
   * Check if a method is read-only.
   * @return whether the 'method' is a read-only operation.
   */
  private static boolean isReadCall(Method method) {
    if (method == null) {
      return false;
    }
    if (!method.isAnnotationPresent(ReadOnly.class)) {
      return false;
    }
    return !method.getAnnotationsByType(ReadOnly.class)[0].activeOnly();
  }

  /**
   * Checks and sets last refresh time for a namespace's stateId.
   * Returns true if refresh time is newer than threshold.
   * Otherwise, return false and call should be handled by active namenode.
   * @param nsId namespaceID
   */
  @VisibleForTesting
  boolean isNamespaceStateIdFresh(String nsId) {
    if (activeNNStateIdRefreshPeriodMs < 0) {
      return true;
    }
    long timeSinceRefreshMs = Time.monotonicNow() - getTimeOfLastCallToActive(nsId).get();
    return (timeSinceRefreshMs <= activeNNStateIdRefreshPeriodMs);
  }

  private LongAccumulator getTimeOfLastCallToActive(String namespaceId) {
    return lastActiveNNRefreshTimes
        .computeIfAbsent(namespaceId, key -> new LongAccumulator(Math::max, 0));
  }

  /**
   * Determine whether router rotated cache is required when NoNamenodesAvailableException occurs.
   *
   * @param ioe cause of the NoNamenodesAvailableException.
   * @return true if NoNamenodesAvailableException occurs due to
   * {@link RouterRpcClient#isUnavailableException(IOException) unavailable exception},
   * otherwise false.
   */
  protected boolean shouldRotateCache(IOException ioe) {
    if (isUnavailableException(ioe)) {
      return true;
    }
    if (ioe instanceof  RemoteException) {
      RemoteException re = (RemoteException) ioe;
      ioe = re.unwrapRemoteException();
      ioe = getCleanException(ioe);
    }
    return isUnavailableException(ioe);
  }


  /**
   * The {@link  ExecutionStatus} class is a utility class used to track the status of
   * execution operations performed by the {@link  RouterRpcClient}.
   * It encapsulates the state of an operation, including whether it has completed,
   * if a failover to a different NameNode should be attempted, and if an observer
   * NameNode should be used for the operation.
   *
   * <p>The status is represented by a flag that indicate the current state of
   * the execution. The flag can be checked individually to determine how to
   * proceed with the operation or to handle its results.
   */
  protected static class ExecutionStatus {

    /** A byte field used to store the state flags. */
    private byte flag;
    private static final byte FAIL_OVER_BIT = 1;
    private static final byte SHOULD_USE_OBSERVER_BIT = 2;
    private static final byte COMPLETE_BIT = 4;

    ExecutionStatus() {
      this(false, false);
    }

    ExecutionStatus(boolean failOver, boolean shouldUseObserver) {
      this.flag = 0;
      setFailOver(failOver);
      setShouldUseObserver(shouldUseObserver);
      setComplete(false);
    }

    private void setFailOver(boolean failOver) {
      flag = (byte) (failOver ? (flag | FAIL_OVER_BIT) : (flag & ~FAIL_OVER_BIT));
    }

    private void setShouldUseObserver(boolean shouldUseObserver) {
      flag = (byte) (shouldUseObserver ?
          (flag | SHOULD_USE_OBSERVER_BIT) : (flag & ~SHOULD_USE_OBSERVER_BIT));
    }

    void setComplete(boolean complete) {
      flag = (byte) (complete ? (flag | COMPLETE_BIT) : (flag & ~COMPLETE_BIT));
    }

    boolean isFailOver() {
      return (flag & FAIL_OVER_BIT) != 0;
    }

    boolean isShouldUseObserver() {
      return (flag &  SHOULD_USE_OBSERVER_BIT) != 0;
    }

    boolean isComplete() {
      return (flag & COMPLETE_BIT) != 0;
    }
  }
}
