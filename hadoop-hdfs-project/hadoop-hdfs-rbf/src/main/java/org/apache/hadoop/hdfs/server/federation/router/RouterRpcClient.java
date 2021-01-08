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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.UserGroupInformation;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
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

  /** Pattern to parse a stack trace line. */
  private static final Pattern STACK_TRACE_PATTERN =
      Pattern.compile("\\tat (.*)\\.(.*)\\((.*):(\\d*)\\)");


  /**
   * Create a router RPC client to manage remote procedure calls to NNs.
   *
   * @param conf Hdfs Configuation.
   * @param router A router using this RPC client.
   * @param resolver A NN resolver to determine the currently active NN in HA.
   * @param monitor Optional performance monitor.
   */
  public RouterRpcClient(Configuration conf, Router router,
      ActiveNamenodeResolver resolver, RouterRpcMonitor monitor) {
    this.router = router;

    this.namenodeResolver = resolver;

    Configuration clientConf = getClientConfiguration(conf);
    this.connectionManager = new ConnectionManager(clientConf);
    this.connectionManager.start();

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
  private ConnectionContext getConnection(UserGroupInformation ugi, String nsId,
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
      if (UserGroupInformation.isSecurityEnabled()) {
        UserGroupInformation routerUser = UserGroupInformation.getLoginUser();
        connUGI = UserGroupInformation.createProxyUser(
            ugi.getUserName(), routerUser);
      }
      connection = this.connectionManager.getConnection(
          connUGI, rpcAddress, proto);
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
   * @return Retry decision.
   * @throws NoNamenodesAvailableException Exception that the retry policy
   *         generates for no available namenodes.
   */
  private RetryDecision shouldRetry(final IOException ioe, final int retryCount,
      final String nsId) throws IOException {
    // check for the case of cluster unavailable state
    if (isClusterUnAvailable(nsId)) {
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
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param ugi User group information.
   * @param namenodes A prioritized list of namenodes within the same
   *                  nameservice.
   * @param method Remote ClientProtcol method to invoke.
   * @param params Variable list of parameters matching the method.
   * @return The result of invoking the method.
   * @throws ConnectException If it cannot connect to any Namenode.
   * @throws StandbyException If all Namenodes are in Standby.
   * @throws IOException If it cannot invoke the method.
   */
  private Object invokeMethod(
      final UserGroupInformation ugi,
      final List<? extends FederationNamenodeContext> namenodes,
      final Class<?> protocol, final Method method, final Object... params)
          throws ConnectException, StandbyException, IOException {

    if (namenodes == null || namenodes.isEmpty()) {
      throw new IOException("No namenodes to invoke " + method.getName() +
          " with params " + Arrays.deepToString(params) + " from "
          + router.getRouterId());
    }

    Object ret = null;
    if (rpcMonitor != null) {
      rpcMonitor.proxyOp();
    }
    boolean failover = false;
    Map<FederationNamenodeContext, IOException> ioes = new LinkedHashMap<>();
    for (FederationNamenodeContext namenode : namenodes) {
      ConnectionContext connection = null;
      String nsId = namenode.getNameserviceId();
      String rpcAddress = namenode.getRpcAddress();
      try {
        connection = this.getConnection(ugi, nsId, rpcAddress, protocol);
        ProxyAndInfo<?> client = connection.getClient();
        final Object proxy = client.getProxy();

        ret = invoke(nsId, 0, method, proxy, params);
        if (failover) {
          // Success on alternate server, update
          InetSocketAddress address = client.getAddress();
          namenodeResolver.updateActiveNamenode(nsId, address);
        }
        if (this.rpcMonitor != null) {
          this.rpcMonitor.proxyOpComplete(true);
        }
        return ret;
      } catch (IOException ioe) {
        ioes.put(namenode, ioe);
        if (ioe instanceof StandbyException) {
          // Fail over indicated by retry policy and/or NN
          if (this.rpcMonitor != null) {
            this.rpcMonitor.proxyOpFailureStandby();
          }
          failover = true;
        } else if (isUnavailableException(ioe)) {
          if (this.rpcMonitor != null) {
            this.rpcMonitor.proxyOpFailureCommunicate();
          }
          failover = true;
        } else if (ioe instanceof RemoteException) {
          if (this.rpcMonitor != null) {
            this.rpcMonitor.proxyOpComplete(true);
          }
          RemoteException re = (RemoteException) ioe;
          ioe = re.unwrapRemoteException();
          ioe = getCleanException(ioe);
          // RemoteException returned by NN
          throw ioe;
        } else if (ioe instanceof ConnectionNullException) {
          if (this.rpcMonitor != null) {
            this.rpcMonitor.proxyOpFailureCommunicate();
          }
          LOG.error("Get connection for {} {} error: {}", nsId, rpcAddress,
              ioe.getMessage());
          // Throw StandbyException so that client can retry
          StandbyException se = new StandbyException(ioe.getMessage());
          se.initCause(ioe);
          throw se;
        } else if (ioe instanceof NoNamenodesAvailableException) {
          if (this.rpcMonitor != null) {
            this.rpcMonitor.proxyOpNoNamenodes();
          }
          LOG.error("Cannot get available namenode for {} {} error: {}",
              nsId, rpcAddress, ioe.getMessage());
          // Throw RetriableException so that client can retry
          throw new RetriableException(ioe);
        } else {
          // Other communication error, this is a failure
          // Communication retries are handled by the retry policy
          if (this.rpcMonitor != null) {
            this.rpcMonitor.proxyOpFailureCommunicate();
            this.rpcMonitor.proxyOpComplete(false);
          }
          throw ioe;
        }
      } finally {
        if (connection != null) {
          connection.release();
        }
      }
    }
    if (this.rpcMonitor != null) {
      this.rpcMonitor.proxyOpComplete(false);
    }

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
   * Invokes a method on the designated object. Catches exceptions specific to
   * the invocation.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param nsId Identifier for the namespace
   * @param retryCount Current retry times
   * @param method Method to invoke
   * @param obj Target object for the method
   * @param params Variable parameters
   * @return Response from the remote server
   * @throws IOException
   * @throws InterruptedException
   */
  private Object invoke(String nsId, int retryCount, final Method method,
      final Object obj, final Object... params) throws IOException {
    try {
      return method.invoke(obj, params);
    } catch (IllegalAccessException e) {
      LOG.error("Unexpected exception while proxying API", e);
      return null;
    } catch (IllegalArgumentException e) {
      LOG.error("Unexpected exception while proxying API", e);
      return null;
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        IOException ioe = (IOException) cause;

        // Check if we should retry.
        RetryDecision decision = shouldRetry(ioe, retryCount, nsId);
        if (decision == RetryDecision.RETRY) {
          if (this.rpcMonitor != null) {
            this.rpcMonitor.proxyOpRetries();
          }

          // retry
          return invoke(nsId, ++retryCount, method, obj, params);
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
   * @param nsId nameservice ID.
   * @return
   * @throws IOException
   */
  private boolean isClusterUnAvailable(String nsId) throws IOException {
    List<? extends FederationNamenodeContext> nnState = this.namenodeResolver
        .getNamenodesForNameserviceId(nsId);

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
  private static IOException getCleanException(IOException ioe) {
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
   * Invokes a ClientProtocol method. Determines the target nameservice via a
   * provided block.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param block Block used to determine appropriate nameservice.
   * @param method The remote method and parameters to invoke.
   * @return The result of invoking the method.
   * @throws IOException If the invoke generated an error.
   */
  public Object invokeSingle(final ExtendedBlock block, RemoteMethod method)
      throws IOException {
    String bpId = block.getBlockPoolId();
    return invokeSingleBlockPool(bpId, method);
  }

  /**
   * Invokes a ClientProtocol method. Determines the target nameservice using
   * the block pool id.
   *
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
   *
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
    List<? extends FederationNamenodeContext> nns =
        getNamenodesForNameservice(nsId);
    RemoteLocationContext loc = new RemoteLocation(nsId, "/", "/");
    Class<?> proto = method.getProtocol();
    Method m = method.getMethod();
    Object[] params = method.getParams(loc);
    return invokeMethod(ugi, nns, proto, m, params);
  }

  /**
   * Invokes a remote method against the specified namespace.
   *
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
   *
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
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param location RemoteLocation to invoke.
   * @param remoteMethod The remote method and parameters to invoke.
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
   * @return The result of the first successful call, or if no calls are
   *         successful, the result of the last RPC call executed.
   * @throws IOException if the success condition is not met and one of the RPC
   *           calls generated a remote exception.
   */
  public Object invokeSequential(
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

    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = remoteMethod.getMethod();
    List<IOException> thrownExceptions = new ArrayList<>();
    Object firstResult = null;
    // Invoke in priority order
    for (final RemoteLocationContext loc : locations) {
      String ns = loc.getNameserviceId();
      List<? extends FederationNamenodeContext> namenodes =
          getNamenodesForNameservice(ns);
      try {
        Class<?> proto = remoteMethod.getProtocol();
        Object[] params = remoteMethod.getParams(loc);
        Object result = invokeMethod(ugi, namenodes, proto, m, params);
        // Check if the result is what we expected
        if (isExpectedClass(expectedResultClass, result) &&
            isExpectedValue(expectedResultValue, result)) {
          // Valid result, stop here
          @SuppressWarnings("unchecked")
          T ret = (T)result;
          return ret;
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
        // ClientProtcol.
        LOG.error("Unexpected exception {} proxying {} to {}",
            e.getClass(), m.getName(), ns, e);
        IOException ioe = new IOException(
            "Unexpected exception proxying API " + e.getMessage(), e);
        thrownExceptions.add(ioe);
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
    @SuppressWarnings("unchecked")
    T ret = (T)firstResult;
    return ret;
  }

  /**
   * Exception messages might contain local subcluster paths. This method
   * generates a new exception with the proper message.
   * @param ioe Original IOException.
   * @param loc Location we are processing.
   * @return Exception processed for federation.
   */
  private IOException processException(
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
  private static boolean isExpectedClass(Class<?> expectedClass, Object clazz) {
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
  private static boolean isExpectedValue(Object expectedValue, Object value) {
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
   * @param <R> The type of the remote method return.
   * @param locations List of remote locations to call concurrently.
   * @param method The remote method and parameters to invoke.
   * @return If the call succeeds in any location.
   * @throws IOException If any of the calls return an exception.
   */
  public <T extends RemoteLocationContext, R> boolean invokeAll(
      final Collection<T> locations, final RemoteMethod method)
          throws IOException {
    boolean anyResult = false;
    Map<T, Boolean> results =
        invokeConcurrent(locations, method, false, false, Boolean.class);
    for (Boolean value : results.values()) {
      boolean result = value.booleanValue();
      if (result) {
        anyResult = true;
      }
    }
    return anyResult;
  }

  /**
   * Invoke multiple concurrent proxy calls to different clients. Returns an
   * array of results.
   *
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
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param <T> The type of the remote location.
   * @param <R> The type of the remote method return.
   * @param locations List of remote locations to call concurrently.
   * @param method The remote method and parameters to invoke.
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
   *
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
   *
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
   *
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
   *
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
      T location = locations.iterator().next();
      String ns = location.getNameserviceId();
      final List<? extends FederationNamenodeContext> namenodes =
          getNamenodesForNameservice(ns);
      try {
        Class<?> proto = method.getProtocol();
        Object[] paramList = method.getParams(location);
        R result = (R) invokeMethod(ugi, namenodes, proto, m, paramList);
        RemoteResult<T, R> remoteResult = new RemoteResult<>(location, result);
        return Collections.singletonList(remoteResult);
      } catch (IOException ioe) {
        // Localize the exception
        throw processException(ioe, location);
      }
    }

    List<T> orderedLocations = new ArrayList<>();
    List<Callable<Object>> callables = new ArrayList<>();
    for (final T location : locations) {
      String nsId = location.getNameserviceId();
      final List<? extends FederationNamenodeContext> namenodes =
          getNamenodesForNameservice(nsId);
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
          callables.add(() -> invokeMethod(ugi, nnList, proto, m, paramList));
        }
      } else {
        // Call the objectGetter in order of nameservices in the NS list
        orderedLocations.add(location);
        callables.add(() ->  invokeMethod(ugi, namenodes, proto, m, paramList));
      }
    }

    if (rpcMonitor != null) {
      rpcMonitor.proxyOp();
    }

    try {
      List<Future<Object>> futures = null;
      if (timeOutMs > 0) {
        futures = executorService.invokeAll(
            callables, timeOutMs, TimeUnit.MILLISECONDS);
      } else {
        futures = executorService.invokeAll(callables);
      }
      List<RemoteResult<T, R>> results = new ArrayList<>();
      for (int i=0; i<futures.size(); i++) {
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
          LOG.debug("Canot execute {} in {}: {}",
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

      return results;
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
    }
  }

  /**
   * Get a prioritized list of NNs that share the same nameservice ID (in the
   * same namespace). NNs that are reported as ACTIVE will be first in the list.
   *
   * @param nsId The nameservice ID for the namespace.
   * @return A prioritized list of NNs to use for communication.
   * @throws IOException If a NN cannot be located for the nameservice ID.
   */
  private List<? extends FederationNamenodeContext> getNamenodesForNameservice(
      final String nsId) throws IOException {

    final List<? extends FederationNamenodeContext> namenodes =
        namenodeResolver.getNamenodesForNameserviceId(nsId);

    if (namenodes == null || namenodes.isEmpty()) {
      throw new IOException("Cannot locate a registered namenode for " + nsId +
          " from " + router.getRouterId());
    }
    return namenodes;
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
}
