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
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.ConnectionContext;
import org.apache.hadoop.hdfs.server.federation.router.RemoteLocationContext;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RemoteResult;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcMonitor;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.router.RouterStateIdContext;
import org.apache.hadoop.hdfs.server.federation.router.ThreadLocalContext;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.ApplyFunction;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncApplyFunction;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncCatchFunction;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.Async.warpCompletionException;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncApplyUseExecutor;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncCatch;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncComplete;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncCompleteWith;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncFinally;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncForEach;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncReturn;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncThrowException;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncTry;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.getCompletableFuture;

/**
 * The {@code RouterAsyncRpcClient} class extends the functionality of the base
 * {@code RouterRpcClient} class to provide asynchronous remote procedure call (RPC)
 * capabilities for communication with the Hadoop Distributed File System (HDFS)
 * NameNodes in a federated environment.
 *
 * <p>This class is responsible for managing the asynchronous execution of RPCs to
 * multiple NameNodes, which can improve performance and scalability in large HDFS
 * deployments.
 *
 * <p>The class also includes methods for handling failover scenarios, where it can
 * automatically retry operations on alternative NameNodes if the primary NameNode is
 * unavailable or in standby mode.
 *
 * @see RouterRpcClient
 */
public class RouterAsyncRpcClient extends RouterRpcClient{
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAsyncRpcClient.class);
  /** Router using this RPC client. */
  private final Router router;
  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private final ActiveNamenodeResolver namenodeResolver;
  /** Optional perf monitor. */
  private final RouterRpcMonitor rpcMonitor;

  /**
   * Create a router async RPC client to manage remote procedure calls to NNs.
   *
   * @param conf Hdfs Configuration.
   * @param router A router using this RPC client.
   * @param resolver A NN resolver to determine the currently active NN in HA.
   * @param monitor Optional performance monitor.
   * @param routerStateIdContext the router state context object to hold the state ids for all
   * namespaces.
   */
  public RouterAsyncRpcClient(Configuration conf,
      Router router, ActiveNamenodeResolver resolver, RouterRpcMonitor monitor,
      RouterStateIdContext routerStateIdContext) {
    super(conf, router, resolver, monitor, routerStateIdContext);
    this.router = router;
    this.namenodeResolver = resolver;
    this.rpcMonitor = monitor;
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
  @Override
  public <T extends RemoteLocationContext> boolean invokeAll(
      final Collection<T> locations, final RemoteMethod method)
      throws IOException {
    invokeConcurrent(locations, method, false, false,
        Boolean.class);
    asyncApply((ApplyFunction<Map<T, Boolean>, Object>)
        results -> results.containsValue(true));
    return asyncReturn(boolean.class);
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
  @Override
  public Object invokeMethod(
      UserGroupInformation ugi,
      List<? extends FederationNamenodeContext> namenodes,
      boolean useObserver, Class<?> protocol,
      Method method, Object... params) throws IOException {
    if (namenodes == null || namenodes.isEmpty()) {
      throw new IOException("No namenodes to invoke " + method.getName() +
          " with params " + Arrays.deepToString(params) + " from "
          + router.getRouterId());
    }
    String nsid = namenodes.get(0).getNameserviceId();
    // transfer threadLocalContext to worker threads of executor.
    ThreadLocalContext threadLocalContext = new ThreadLocalContext();
    asyncComplete(null);
    asyncApplyUseExecutor((AsyncApplyFunction<Object, Object>) o -> {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Async invoke method : {}, {}, {}, {}", method.getName(), useObserver,
            namenodes.toString(), params);
      }
      threadLocalContext.transfer();
      invokeMethodAsync(ugi, (List<FederationNamenodeContext>) namenodes,
          useObserver, protocol, method, params);
    }, router.getRpcServer().getAsyncRouterHandlerExecutors().getOrDefault(nsid,
        router.getRpcServer().getRouterAsyncHandlerDefaultExecutor()));
    return null;
  }

  /**
   * Asynchronously invokes a method on the specified NameNodes for a given user and operation.
   * This method is responsible for the actual execution of the remote method call on the
   * NameNodes in a non-blocking manner, allowing for concurrent processing.
   *
   * <p>In case of exceptions, the method includes logic to handle retries, failover to standby
   * NameNodes, and proper exception handling to ensure that the calling code can respond
   * appropriately to different error conditions.
   *
   * @param ugi The user information under which the method is to be invoked.
   * @param namenodes The list of NameNode contexts on which the method will be invoked.
   * @param useObserver Whether to use an observer node for the invocation if available.
   * @param protocol The protocol class defining the method to be invoked.
   * @param method The method to be invoked on the NameNodes.
   * @param params The parameters for the method invocation.
   */
  private void invokeMethodAsync(
      final UserGroupInformation ugi,
      final List<FederationNamenodeContext> namenodes,
      boolean useObserver,
      final Class<?> protocol, final Method method, final Object... params) {

    addClientInfoToCallerContext(ugi);
    if (rpcMonitor != null) {
      rpcMonitor.proxyOp();
    }
    final ExecutionStatus status = new ExecutionStatus(false, useObserver);
    Map<FederationNamenodeContext, IOException> ioes = new LinkedHashMap<>();
    final ConnectionContext[] connection = new ConnectionContext[1];
    asyncForEach(namenodes.iterator(),
        (foreach, namenode) -> {
          if (!status.isShouldUseObserver()
              && (namenode.getState() == FederationNamenodeServiceState.OBSERVER)) {
            asyncComplete(null);
            return;
          }
          String nsId = namenode.getNameserviceId();
          String rpcAddress = namenode.getRpcAddress();
          asyncTry(() -> {
            connection[0] = getConnection(ugi, nsId, rpcAddress, protocol);
            NameNodeProxiesClient.ProxyAndInfo<?> client = connection[0].getClient();
            invoke(namenode, status.isShouldUseObserver(), 0, method,
                  client.getProxy(), params);
            asyncApply(res -> {
              status.setComplete(true);
              postProcessResult(method, status, namenode, nsId, client);
              foreach.breakNow();
              return res;
            });
          });
          asyncCatch((res, ioe) -> {
            ioes.put(namenode, ioe);
            handleInvokeMethodIOException(namenode, ioe, status, useObserver);
            return res;
          }, IOException.class);
          asyncFinally(res -> {
            if (connection[0] != null) {
              connection[0].release();
            }
            return res;
          });
        });

    asyncApply(res -> {
      if (status.isComplete()) {
        return res;
      }
      return handlerAllNamenodeFail(namenodes, method, ioes, params);
    });
  }

  /**
   * Asynchronously invokes a method on a specified NameNode in the context of the given
   * namespace and NameNode information. This method is designed to handle the invocation
   * in a non-blocking manner, allowing for improved performance and scalability when
   * interacting with the NameNode.
   *
   * @param namenode The context information for the NameNode.
   * @param listObserverFirst Whether to list the observer node first in the invocation list.
   * @param retryCount The current retry count for the operation.
   * @param method The method to be invoked on the NameNode.
   * @param obj The proxy object through which the method will be invoked.
   * @param params The parameters for the method invocation.
   */
  protected Object invoke(
      FederationNamenodeContext namenode, Boolean listObserverFirst,
      int retryCount, final Method method,
      final Object obj, final Object... params) throws IOException {
    try {
      Client.setAsynchronousMode(true);
      method.invoke(obj, params);
      Client.setAsynchronousMode(false);
      asyncCatch((AsyncCatchFunction<Object, Throwable>) (o, e) -> {
        handlerInvokeException(namenode, listObserverFirst,
            retryCount, method, obj, e, params);
      }, Throwable.class);
    } catch (InvocationTargetException e) {
      asyncThrowException(e.getCause());
    } catch (IllegalAccessException | IllegalArgumentException e) {
      LOG.error("Unexpected exception while proxying API", e);
      asyncThrowException(e);
    }
    return null;
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
  @Override
  public <T> T invokeSequential(
      final List<? extends RemoteLocationContext> locations,
      final RemoteMethod remoteMethod, Class<T> expectedResultClass,
      Object expectedResultValue) throws IOException {
    invokeSequential(remoteMethod, locations, expectedResultClass, expectedResultValue);
    asyncApply((ApplyFunction<RemoteResult, Object>) RemoteResult::getResult);
    return asyncReturn(expectedResultClass);
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
  @Override
  public <R extends RemoteLocationContext, T> RemoteResult invokeSequential(
      final RemoteMethod remoteMethod, final List<R> locations,
      Class<T> expectedResultClass, Object expectedResultValue)
      throws IOException {

    RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = remoteMethod.getMethod();
    List<IOException> thrownExceptions = new ArrayList<>();
    final Object[] firstResult = {null};
    final ExecutionStatus status = new ExecutionStatus();
    Iterator<RemoteLocationContext> locationIterator =
        (Iterator<RemoteLocationContext>) locations.iterator();
    // Invoke in priority order
    asyncForEach(locationIterator,
        (foreach, loc) -> {
          String ns = loc.getNameserviceId();
          boolean isObserverRead = isObserverReadEligible(ns, m);
          List<? extends FederationNamenodeContext> namenodes =
              getOrderedNamenodes(ns, isObserverRead);
          acquirePermit(ns, ugi, remoteMethod, controller);
          asyncTry(() -> {
            Class<?> proto = remoteMethod.getProtocol();
            Object[] params = remoteMethod.getParams(loc);
            invokeMethod(ugi, namenodes, isObserverRead, proto, m, params);
            asyncApply(result -> {
              // Check if the result is what we expected
              if (isExpectedClass(expectedResultClass, result) &&
                  isExpectedValue(expectedResultValue, result)) {
                // Valid result, stop here
                @SuppressWarnings("unchecked") R location = (R) loc;
                @SuppressWarnings("unchecked") T ret = (T) result;
                foreach.breakNow();
                status.setComplete(true);
                return new RemoteResult<>(location, ret);
              }
              if (firstResult[0] == null) {
                firstResult[0] = result;
              }
              return null;
            });
          });
          asyncCatch((ret, e) -> {
            if (e instanceof IOException) {
              IOException ioe = (IOException) e;
              // Localize the exception
              ioe = processException(ioe, loc);
              // Record it and move on
              thrownExceptions.add(ioe);
            } else {
              // Unusual error, ClientProtocol calls always use IOException (or
              // RemoteException). Re-wrap in IOException for compatibility with
              // ClientProtocol.
              LOG.error("Unexpected exception {} proxying {} to {}",
                  e.getClass(), m.getName(), ns, e);
              IOException ioe = new IOException(
                  "Unexpected exception proxying API " + e.getMessage(), e);
              thrownExceptions.add(ioe);
            }
            return ret;
          }, Exception.class);
          asyncFinally(ret -> {
            releasePermit(ns, ugi, remoteMethod, controller);
            return ret;
          });
        });
    asyncApply(result -> {
      if (status.isComplete()) {
        return result;
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
      @SuppressWarnings("unchecked") T ret = (T) firstResult[0];
      return new RemoteResult<>(locations.get(0), ret);
    });
    return asyncReturn(RemoteResult.class);
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
  @Override
  public <T extends RemoteLocationContext, R> Map<T, R> invokeConcurrent(
      final Collection<T> locations, final RemoteMethod method,
      boolean requireResponse, boolean standby, long timeOutMs, Class<R> clazz)
      throws IOException {
    invokeConcurrent(locations, method, standby, timeOutMs, clazz);
    asyncApply((ApplyFunction<List<RemoteResult<T, R>>, Object>)
        results -> postProcessResult(requireResponse, results));
    return asyncReturn(Map.class);
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
  @Override
  protected <T extends RemoteLocationContext, R> List<RemoteResult<T, R>> getRemoteResults(
      RemoteMethod method, long timeOutMs, RouterRpcFairnessPolicyController controller,
      List<T> orderedLocations, List<Callable<Object>> callables) throws IOException {
    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = method.getMethod();
    final CompletableFuture<Object>[] futures =
        new CompletableFuture[callables.size()];
    int i = 0;
    for (Callable<Object> callable : callables) {
      CompletableFuture<Object> future = null;
      try {
        callable.call();
        future = getCompletableFuture();
      } catch (Exception e) {
        future = new CompletableFuture<>();
        future.completeExceptionally(warpCompletionException(e));
      }
      futures[i++] = future;
    }

    asyncCompleteWith(CompletableFuture.allOf(futures)
        .handle((unused, throwable) -> {
          try {
            return processFutures(method, m, orderedLocations, Arrays.asList(futures));
          } catch (InterruptedException e) {
            LOG.error("Unexpected error while invoking API: {}", e.getMessage());
            throw warpCompletionException(new IOException(
                "Unexpected error while invoking API " + e.getMessage(), e));
          } finally {
            releasePermit(CONCURRENT_NS, ugi, method, controller);
          }
        }));
    return asyncReturn(List.class);
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
  @Override
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
    asyncTry(() -> {
      Class<?> proto = method.getProtocol();
      Object[] paramList = method.getParams(location);
      invokeMethod(ugi, namenodes, isObserverRead, proto, m, paramList);
      asyncApply((ApplyFunction<R, Object>) result -> {
        RemoteResult<T, R> remoteResult = new RemoteResult<>(location, result);
        return Collections.singletonList(remoteResult);
      });
    });
    asyncCatch((o, ioe) -> {
      throw processException(ioe, location);
    }, IOException.class);
    asyncFinally(o -> {
      releasePermit(ns, ugi, method, controller);
      return o;
    });
    return asyncReturn(List.class);
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
  @Override
  public Object invokeSingle(final String nsId, RemoteMethod method)
      throws IOException {
    UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
    acquirePermit(nsId, ugi, method, controller);
    asyncTry(() -> {
      boolean isObserverRead = isObserverReadEligible(nsId, method.getMethod());
      List<? extends FederationNamenodeContext> nns = getOrderedNamenodes(nsId, isObserverRead);
      RemoteLocationContext loc = new RemoteLocation(nsId, "/", "/");
      Class<?> proto = method.getProtocol();
      Method m = method.getMethod();
      Object[] params = method.getParams(loc);
      invokeMethod(ugi, nns, isObserverRead, proto, m, params);
    });
    asyncFinally(o -> {
      releasePermit(nsId, ugi, method, controller);
      return o;
    });
    return null;
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
  public <T> T invokeSingle(
      final RemoteLocationContext location,
      RemoteMethod remoteMethod, Class<T> clazz) throws IOException {
    List<RemoteLocationContext> locations = Collections.singletonList(location);
    invokeSequential(locations, remoteMethod);
    return asyncReturn(clazz);
  }
}
