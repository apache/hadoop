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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A client proxy for Router -> NN communication using the NN ClientProtocol.
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


  /** Router identifier. */
  private final String routerId;

  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private final ActiveNamenodeResolver namenodeResolver;

  /** Connection pool to the Namenodes per user for performance. */
  private final ConnectionManager connectionManager;
  /** Service to run asynchronous calls. */
  private final ExecutorService executorService;
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
   * @param resolver A NN resolver to determine the currently active NN in HA.
   * @param monitor Optional performance monitor.
   */
  public RouterRpcClient(Configuration conf, String identifier,
      ActiveNamenodeResolver resolver, RouterRpcMonitor monitor) {
    this.routerId = identifier;

    this.namenodeResolver = resolver;

    this.connectionManager = new ConnectionManager(conf);
    this.connectionManager.start();

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("RPC Router Client-%d")
        .build();
    this.executorService = Executors.newCachedThreadPool(threadFactory);

    this.rpcMonitor = monitor;

    int maxFailoverAttempts = conf.getInt(
        HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_KEY,
        HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_DEFAULT);
    int maxRetryAttempts = conf.getInt(
        HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY,
        HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_DEFAULT);
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
   * Get ClientProtocol proxy client for a NameNode. Each combination of user +
   * NN must use a unique proxy client. Previously created clients are cached
   * and stored in a connection pool by the ConnectionManager.
   *
   * @param ugi User group information.
   * @param nsId Nameservice identifier.
   * @param rpcAddress ClientProtocol RPC server address of the NN.
   * @return ConnectionContext containing a ClientProtocol proxy client for the
   *         NN + current user.
   * @throws IOException If we cannot get a connection to the NameNode.
   */
  private ConnectionContext getConnection(
      UserGroupInformation ugi, String nsId, String rpcAddress)
          throws IOException {
    ConnectionContext connection = null;
    try {
      // Each proxy holds the UGI info for the current user when it is created.
      // This cache does not scale very well, one entry per user per namenode,
      // and may need to be adjusted and/or selectively pruned. The cache is
      // important due to the excessive overhead of creating a new proxy wrapper
      // for each individual request.

      // TODO Add tokens from the federated UGI
      connection = this.connectionManager.getConnection(ugi, rpcAddress);
      LOG.debug("User {} NN {} is using connection {}",
          ugi.getUserName(), rpcAddress, connection);
    } catch (Exception ex) {
      LOG.error("Cannot open NN client to address: {}", rpcAddress, ex);
    }

    if (connection == null) {
      throw new IOException("Cannot get a connection to " + rpcAddress);
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
   * @return Retry decision.
   * @throws IOException Original exception if the retry policy generates one.
   */
  private RetryDecision shouldRetry(final IOException ioe, final int retryCount)
      throws IOException {
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
   * @throws IOException
   */
  private Object invokeMethod(
      final UserGroupInformation ugi,
      final List<? extends FederationNamenodeContext> namenodes,
      final Method method, final Object... params) throws IOException {

    if (namenodes == null || namenodes.isEmpty()) {
      throw new IOException("No namenodes to invoke " + method.getName() +
          " with params " + Arrays.toString(params) + " from " + this.routerId);
    }

    Object ret = null;
    if (rpcMonitor != null) {
      rpcMonitor.proxyOp();
    }
    boolean failover = false;
    Map<FederationNamenodeContext, IOException> ioes = new LinkedHashMap<>();
    for (FederationNamenodeContext namenode : namenodes) {
      ConnectionContext connection = null;
      try {
        String nsId = namenode.getNameserviceId();
        String rpcAddress = namenode.getRpcAddress();
        connection = this.getConnection(ugi, nsId, rpcAddress);
        ProxyAndInfo<ClientProtocol> client = connection.getClient();
        ClientProtocol proxy = client.getProxy();
        ret = invoke(0, method, proxy, params);
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
        } else if (ioe instanceof RemoteException) {
          if (this.rpcMonitor != null) {
            this.rpcMonitor.proxyOpComplete(true);
          }
          // RemoteException returned by NN
          throw (RemoteException) ioe;
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
        Arrays.toString(params);
    LOG.error(msg);
    for (Entry<FederationNamenodeContext, IOException> entry :
        ioes.entrySet()) {
      FederationNamenodeContext namenode = entry.getKey();
      String nsId = namenode.getNameserviceId();
      String nnId = namenode.getNamenodeId();
      String addr = namenode.getRpcAddress();
      IOException ioe = entry.getValue();
      if (ioe instanceof StandbyException) {
        LOG.error("{} {} at {} is in Standby", nsId, nnId, addr);
      } else {
        LOG.error("{} {} at {} error: \"{}\"",
            nsId, nnId, addr, ioe.getMessage());
      }
    }
    throw new StandbyException(msg);
  }

  /**
   * Invokes a method on the designated object. Catches exceptions specific to
   * the invocation.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param method Method to invoke
   * @param obj Target object for the method
   * @param params Variable parameters
   * @return Response from the remote server
   * @throws IOException
   * @throws InterruptedException
   */
  private Object invoke(int retryCount, final Method method, final Object obj,
      final Object... params) throws IOException {
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
        RetryDecision decision = shouldRetry(ioe, retryCount);
        if (decision == RetryDecision.RETRY) {
          // retry
          return invoke(++retryCount, method, obj, params);
        } else if (decision == RetryDecision.FAILOVER_AND_RETRY) {
          // failover, invoker looks for standby exceptions for failover.
          if (ioe instanceof StandbyException) {
            throw ioe;
          } else {
            throw new StandbyException(ioe.getMessage());
          }
        } else {
          if (ioe instanceof RemoteException) {
            RemoteException re = (RemoteException) ioe;
            ioe = re.unwrapRemoteException();
            ioe = getCleanException(ioe);
          }
          throw ioe;
        }
      } else {
        throw new IOException(e);
      }
    }
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
   * @throws IOException
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
   * @throws IOException
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
   * @throws IOException
   */
  public Object invokeSingle(final String nsId, RemoteMethod method)
      throws IOException {
    UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    List<? extends FederationNamenodeContext> nns =
        getNamenodesForNameservice(nsId);
    RemoteLocationContext loc = new RemoteLocation(nsId, "/");
    return invokeMethod(ugi, nns, method.getMethod(), method.getParams(loc));
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
   * @throws IOException
   */
  public Object invokeSingle(final RemoteLocationContext location,
      RemoteMethod remoteMethod) throws IOException {
    List<RemoteLocationContext> locations = Collections.singletonList(location);
    return invokeSequential(locations, remoteMethod);
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
  public Object invokeSequential(
      final List<? extends RemoteLocationContext> locations,
      final RemoteMethod remoteMethod, Class<?> expectedResultClass,
      Object expectedResultValue) throws IOException {

    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = remoteMethod.getMethod();
    IOException firstThrownException = null;
    IOException lastThrownException = null;
    Object firstResult = null;
    // Invoke in priority order
    for (final RemoteLocationContext loc : locations) {
      String ns = loc.getNameserviceId();
      List<? extends FederationNamenodeContext> namenodes =
          getNamenodesForNameservice(ns);
      try {
        Object[] params = remoteMethod.getParams(loc);
        Object result = invokeMethod(ugi, namenodes, m, params);
        // Check if the result is what we expected
        if (isExpectedClass(expectedResultClass, result) &&
            isExpectedValue(expectedResultValue, result)) {
          // Valid result, stop here
          return result;
        }
        if (firstResult == null) {
          firstResult = result;
        }
      } catch (IOException ioe) {
        // Record it and move on
        lastThrownException = (IOException) ioe;
        if (firstThrownException == null) {
          firstThrownException = lastThrownException;
        }
      } catch (Exception e) {
        // Unusual error, ClientProtocol calls always use IOException (or
        // RemoteException). Re-wrap in IOException for compatibility with
        // ClientProtcol.
        LOG.error("Unexpected exception {} proxying {} to {}",
            e.getClass(), m.getName(), ns, e);
        lastThrownException = new IOException(
            "Unexpected exception proxying API " + e.getMessage(), e);
        if (firstThrownException == null) {
          firstThrownException = lastThrownException;
        }
      }
    }

    if (firstThrownException != null) {
      // re-throw the last exception thrown for compatibility
      throw firstThrownException;
    }
    // Return the last result, whether it is the value we are looking for or a
    return firstResult;
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
   * Invokes multiple concurrent proxy calls to different clients. Returns an
   * array of results.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param <T> The type of the remote location.
   * @param locations List of remote locations to call concurrently.
   * @param method The remote method and parameters to invoke.
   * @param requireResponse If true an exception will be thrown if all calls do
   *          not complete. If false exceptions are ignored and all data results
   *          successfully received are returned.
   * @param standby If the requests should go to the standby namenodes too.
   * @return Result of invoking the method per subcluster: nsId -> result.
   * @throws IOException If requiredResponse=true and any of the calls throw an
   *           exception.
   */
  public <T extends RemoteLocationContext> Map<T, Object> invokeConcurrent(
      final Collection<T> locations, final RemoteMethod method,
      boolean requireResponse, boolean standby) throws IOException {
    return invokeConcurrent(locations, method, requireResponse, standby, -1);
  }

  /**
   * Invokes multiple concurrent proxy calls to different clients. Returns an
   * array of results.
   *
   * Re-throws exceptions generated by the remote RPC call as either
   * RemoteException or IOException.
   *
   * @param locations List of remote locations to call concurrently.
   * @param method The remote method and parameters to invoke.
   * @param requireResponse If true an exception will be thrown if all calls do
   *          not complete. If false exceptions are ignored and all data results
   *          successfully received are returned.
   * @param standby If the requests should go to the standby namenodes too.
   * @param timeOutMs Timeout for each individual call.
   * @return Result of invoking the method per subcluster: nsId -> result.
   * @throws IOException If requiredResponse=true and any of the calls throw an
   *           exception.
   */
  @SuppressWarnings("unchecked")
  public <T extends RemoteLocationContext> Map<T, Object> invokeConcurrent(
      final Collection<T> locations, final RemoteMethod method,
      boolean requireResponse, boolean standby, long timeOutMs)
          throws IOException {

    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = method.getMethod();

    if (locations.size() == 1) {
      // Shortcut, just one call
      T location = locations.iterator().next();
      String ns = location.getNameserviceId();
      final List<? extends FederationNamenodeContext> namenodes =
          getNamenodesForNameservice(ns);
      Object[] paramList = method.getParams(location);
      Object result = invokeMethod(ugi, namenodes, m, paramList);
      return Collections.singletonMap(location, result);
    }

    List<T> orderedLocations = new LinkedList<>();
    Set<Callable<Object>> callables = new HashSet<>();
    for (final T location : locations) {
      String nsId = location.getNameserviceId();
      final List<? extends FederationNamenodeContext> namenodes =
          getNamenodesForNameservice(nsId);
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
          callables.add(new Callable<Object>() {
            public Object call() throws Exception {
              return invokeMethod(ugi, nnList, m, paramList);
            }
          });
        }
      } else {
        // Call the objectGetter in order of nameservices in the NS list
        orderedLocations.add(location);
        callables.add(new Callable<Object>() {
          public Object call() throws Exception {
            return invokeMethod(ugi, namenodes, m, paramList);
          }
        });
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
      Map<T, Object> results = new TreeMap<>();
      Map<T, IOException> exceptions = new TreeMap<>();
      for (int i=0; i<futures.size(); i++) {
        T location = orderedLocations.get(i);
        try {
          Future<Object> future = futures.get(i);
          Object result = future.get();
          results.put(location, result);
        } catch (CancellationException ce) {
          T loc = orderedLocations.get(i);
          String msg =
              "Invocation to \"" + loc + "\" for \"" + method + "\" timed out";
          LOG.error(msg);
          IOException ioe = new IOException(msg);
          exceptions.put(location, ioe);
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

          // Response from all servers required, use this error.
          if (requireResponse) {
            throw ioe;
          }

          // Store the exceptions
          exceptions.put(location, ioe);
        }
      }

      // Throw the exception for the first location if there are no results
      if (results.isEmpty()) {
        T location = orderedLocations.get(0);
        IOException ioe = exceptions.get(location);
        if (ioe != null) {
          throw ioe;
        }
      }

      return results;
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
          " from " + this.routerId);
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
          " from " + this.routerId);
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
