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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient;
import org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCPerformanceMonitor.CONCURRENT;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.CUR_COMPLETABLE_FUTURE;
import static org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer.getAsyncRouterHandler;

public class RouterAsyncRpcClient extends RouterRpcClient{
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAsyncRpcClient.class);

  /**
   * Create a router RPC client to manage remote procedure calls to NNs.
   *
   * @param conf                 Hdfs Configuration.
   * @param router               A router using this RPC client.
   * @param resolver             A NN resolver to determine the currently active NN in HA.
   * @param monitor              Optional performance monitor.
   * @param routerStateIdContext the router state context object to hold the state ids for all
   *                             namespaces.
   */
  public RouterAsyncRpcClient(
      Configuration conf, Router router, ActiveNamenodeResolver resolver,
      RouterRpcMonitor monitor, RouterStateIdContext routerStateIdContext) {
    super(conf, router, resolver, monitor, routerStateIdContext);
  }

  @Override
  public <T extends RemoteLocationContext> boolean invokeAll(
      final Collection<T> locations, final RemoteMethod method)
      throws IOException {
    invokeConcurrent(locations, method, false, false, Boolean.class);
    CompletableFuture<Object> completableFuture = CUR_COMPLETABLE_FUTURE.get();
    completableFuture = completableFuture.thenApply(o -> {
      Map<T, Boolean> results = (Map<T, Boolean>) o;
      return results.containsValue(true);
    });
    CUR_COMPLETABLE_FUTURE.set(completableFuture);
//    return (boolean) getResult();
    return false;
  }

  @Override
  public Object invokeMethod(
      UserGroupInformation ugi,
      List<? extends FederationNamenodeContext> namenodes,
      boolean useObserver, Class<?> protocol,
      Method method, Object... params) throws IOException {
    CompletableFuture<Object> completableFuture =
        CompletableFuture.completedFuture(null);
    // transfer originCall & callerContext to worker threads of executor.
    final Server.Call originCall = Server.getCurCall().get();
    final CallerContext originContext = CallerContext.getCurrent();
    completableFuture = completableFuture.thenComposeAsync(o -> {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Async invoke method : {}, {}, {}, {}", method.getName(), useObserver,
            namenodes.toString(), params);
      }
      transferThreadLocalContext(originCall, originContext);
      try {
        return invokeMethodAsync(ugi, namenodes, useObserver, protocol, method, params);
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    }, getAsyncRouterHandler());
    CUR_COMPLETABLE_FUTURE.set(completableFuture);
    return completableFuture;
  }

  private CompletableFuture<Object> invokeMethodAsync(
      final UserGroupInformation ugi,
      final List<? extends FederationNamenodeContext> namenodes,
      boolean useObserver,
      final Class<?> protocol, final Method method, final Object... params)
      throws IOException {

    if (namenodes == null || namenodes.isEmpty()) {
      throw new IOException("No namenodes to invoke " + method.getName() +
          " with params " + Arrays.deepToString(params) + " from "
          + router.getRouterId());
    }

    addClientInfoToCallerContext(ugi);
    if (rpcMonitor != null) {
      rpcMonitor.incrProcessingOp();
    }
    // transfer originCall & callerContext to worker threads of executor.
    final Server.Call originCall = Server.getCurCall().get();
    final CallerContext originContext = CallerContext.getCurrent();

    final long startProxyTime = Time.monotonicNow();
    Map<FederationNamenodeContext, IOException> ioes = new LinkedHashMap<>();

    CompletableFuture<Object[]> completableFuture =
        CompletableFuture.completedFuture(new Object[]{useObserver, false, false, null});

    for (FederationNamenodeContext namenode : namenodes) {
      completableFuture = completableFuture.thenCompose(args -> {
        Boolean shouldUseObserver = (Boolean) args[0];
        Boolean failover = (Boolean) args[1];
        Boolean complete = (Boolean) args[2];
        if (complete) {
          return CompletableFuture.completedFuture(
              new Object[]{shouldUseObserver, failover, complete, args[3]});
        }
        return invokeAsyncTask(originCall, originContext, startProxyTime, ioes, ugi,
            namenode, shouldUseObserver, failover, protocol, method, params);
      });
    }

    return completableFuture.thenApply(args -> {
      Boolean complete = (Boolean) args[2];
      if (complete) {
        return args[3];
      }
      // All namenodes were unavailable or in standby
      String msg = "No namenode available to invoke " + method.getName() + " " +
          Arrays.deepToString(params) + " in " + namenodes + " from " +
          router.getRouterId();
      LOG.error(msg);
      int exConnect = 0;
      for (Map.Entry<FederationNamenodeContext, IOException> entry :
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
        throw new CompletionException(new ConnectException(msg));
      } else {
        throw new CompletionException(new StandbyException(msg));
      }
    });
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private CompletableFuture<Object[]> invokeAsyncTask(
      final Server.Call originCall,
      final CallerContext callerContext,
      final long startProxyTime,
      final Map<FederationNamenodeContext, IOException> ioes,
      final UserGroupInformation ugi,
      FederationNamenodeContext namenode,
      boolean useObserver,
      boolean failover,
      final Class<?> protocol, final Method method, final Object... params) {
    transferThreadLocalContext(originCall, callerContext);
    if (!useObserver && (namenode.getState() == FederationNamenodeServiceState.OBSERVER)) {
      return CompletableFuture.completedFuture(new Object[]{useObserver, failover, false, null});
    }
    String nsId = namenode.getNameserviceId();
    String rpcAddress = namenode.getRpcAddress();
    try {
      ConnectionContext connection = getConnection(ugi, nsId, rpcAddress, protocol);
      NameNodeProxiesClient.ProxyAndInfo<?> client = connection.getClient();
      return invokeAsync(originCall, callerContext, nsId, namenode, useObserver,
          0, method, client.getProxy(), params)
          .handle((result, e) -> {
            connection.release();
            boolean complete = false;
            if (result != null || e == null) {
              complete = true;
              if (failover &&
                  FederationNamenodeServiceState.OBSERVER != namenode.getState()) {
                // Success on alternate server, update
                InetSocketAddress address = client.getAddress();
                try {
                  namenodeResolver.updateActiveNamenode(nsId, address);
                } catch (IOException ex) {
                  throw new CompletionException(ex);
                }
              }
              if (this.rpcMonitor != null) {
                this.rpcMonitor.proxyOpComplete(
                    true, nsId, namenode.getState(), Time.monotonicNow() - startProxyTime);
              }
              if (this.router.getRouterClientMetrics() != null) {
                this.router.getRouterClientMetrics().incInvokedMethod(method);
              }
              return new Object[] {useObserver, failover, complete, result};
            }
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
              IOException ioe = (IOException) cause;
              ioes.put(namenode, ioe);
              if (ioe instanceof ObserverRetryOnActiveException) {
                LOG.info("Encountered ObserverRetryOnActiveException from {}."
                    + " Retry active namenode directly.", namenode);
                return new Object[]{false, failover, complete, null};
              } else if (ioe instanceof StandbyException) {
                // Fail over indicated by retry policy and/or NN
                if (this.rpcMonitor != null) {
                  this.rpcMonitor.proxyOpFailureStandby(nsId);
                }
                return new Object[]{useObserver, true, complete, null};
              } else if (isUnavailableException(ioe)) {
                if (this.rpcMonitor != null) {
                  this.rpcMonitor.proxyOpFailureCommunicate(nsId);
                }
                boolean tmpFailover = failover;
                if (FederationNamenodeServiceState.OBSERVER == namenode.getState()) {
                  try {
                    namenodeResolver.updateUnavailableNamenode(nsId,
                        NetUtils.createSocketAddr(namenode.getRpcAddress()));
                  } catch (IOException ex) {
                    throw new CompletionException(ex);
                  }
                } else {
                  tmpFailover = true;
                }
                return new Object[]{useObserver, tmpFailover, complete, null};
              } else if (ioe instanceof RemoteException) {
                if (this.rpcMonitor != null) {
                  this.rpcMonitor.proxyOpComplete(
                      true, nsId, namenode.getState(), Time.monotonicNow() - startProxyTime);
                }
                RemoteException re = (RemoteException) ioe;
                ioe = re.unwrapRemoteException();
                ioe = getCleanException(ioe);
                // RemoteException returned by NN
                throw new CompletionException(ioe);
              } else if (ioe instanceof NoNamenodesAvailableException) {
                IOException cau = (IOException) ioe.getCause();
                if (this.rpcMonitor != null) {
                  this.rpcMonitor.proxyOpNoNamenodes(nsId);
                }
                LOG.error("Cannot get available namenode for {} {} error: {}",
                    nsId, rpcAddress, ioe.getMessage());
                // Rotate cache so that client can retry the next namenode in the cache
                if (shouldRotateCache(cau)) {
                  this.namenodeResolver.rotateCache(nsId, namenode, useObserver);
                }
                // Throw RetriableException so that client can retry
                throw new CompletionException(new RetriableException(ioe));
              } else {
                // Other communication error, this is a failure
                // Communication retries are handled by the retry policy
                if (this.rpcMonitor != null) {
                  this.rpcMonitor.proxyOpFailureCommunicate(nsId);
                  this.rpcMonitor.proxyOpComplete(
                      false, nsId, namenode.getState(), Time.monotonicNow() - startProxyTime);
                }
                throw new CompletionException(ioe);
              }
            }
            throw new CompletionException(cause);
          });
    }catch (IOException ioe) {
      assert ioe instanceof ConnectionNullException;
      if (this.rpcMonitor != null) {
        this.rpcMonitor.proxyOpFailureCommunicate(nsId);
      }
      LOG.error("Get connection for {} {} error: {}", nsId, rpcAddress,
          ioe.getMessage());
      // Throw StandbyException so that client can retry
      StandbyException se = new StandbyException(ioe.getMessage());
      se.initCause(ioe);
      throw new CompletionException(se);
    }
  }


  @SuppressWarnings("checkstyle:ParameterNumber")
  private CompletableFuture<Object> invokeAsync(
      final Server.Call originCall,
      final CallerContext callerContext,
      String nsId, FederationNamenodeContext namenode,
      Boolean listObserverFirst,
      int retryCount, final Method method,
      final Object obj, final Object... params) {
    try {
      transferThreadLocalContext(originCall, callerContext);
      Client.setAsynchronousMode(true);
      method.invoke(obj, params);
      // so unset the value , ensure dfsclient rpc is sync.
      Client.setAsynchronousMode(false);
      CompletableFuture<Object> completableFuture =
          AsyncRpcProtocolPBUtil.getCompletableFuture();

      return completableFuture.handle((BiFunction<Object, Throwable, Object>) (result, e) -> {
        if (e == null) {
          return new Object[]{result, true};
        }

        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          IOException ioe = (IOException) cause;

          // Check if we should retry.
          RetryPolicy.RetryAction.RetryDecision decision = null;
          try {
            decision = shouldRetry(ioe, retryCount, nsId, namenode, listObserverFirst);
          } catch (IOException ex) {
            throw new CompletionException(ex);
          }
          if (decision == RetryPolicy.RetryAction.RetryDecision.RETRY) {
            if (RouterAsyncRpcClient.this.rpcMonitor != null) {
              RouterAsyncRpcClient.this.rpcMonitor.proxyOpRetries();
            }
            // retry
            return new Object[]{result, false};
          } else if (decision == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY) {
            // failover, invoker looks for standby exceptions for failover.
            if (ioe instanceof StandbyException) {
              throw new CompletionException(ioe);
            } else if (isUnavailableException(ioe)) {
              throw new CompletionException(ioe);
            } else {
              throw new CompletionException(new StandbyException(ioe.getMessage()));
            }
          } else {
            throw new CompletionException(ioe);
          }
        } else {
          throw new CompletionException(new IOException(e));
        }
      }).thenCompose(o -> {
        Object[] args = (Object[]) o;
        boolean complete = (boolean) args[1];
        if (complete) {
          return CompletableFuture.completedFuture(args[0]);
        }
        return invokeAsync(originCall, callerContext, nsId, namenode,
            listObserverFirst, retryCount + 1, method, obj, params);
      });
    } catch (InvocationTargetException e) {
      throw new CompletionException(e.getCause());
    } catch (Exception e) {
      throw new CompletionException(e);
    }
  }

  @Override
  public <T> T invokeSequential(
      final List<? extends RemoteLocationContext> locations,
      final RemoteMethod remoteMethod, Class<T> expectedResultClass,
      Object expectedResultValue) throws IOException {
    invokeSequential(remoteMethod, locations, expectedResultClass, expectedResultValue);
    CompletableFuture<Object> completableFuture = CUR_COMPLETABLE_FUTURE.get();
    completableFuture = completableFuture.thenApply(o -> {
      RemoteResult result = (RemoteResult) o;
      return result.getResult();
    });
    CUR_COMPLETABLE_FUTURE.set(completableFuture);
//    return (T) getResult();
    return RouterAsyncRpcUtil.asyncReturn(expectedResultClass);
  }

  @Override
  public <R extends RemoteLocationContext, T> RemoteResult invokeSequential(
      final RemoteMethod remoteMethod, final List<R> locations,
      Class<T> expectedResultClass, Object expectedResultValue)
      throws IOException {
    RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
    final UserGroupInformation ugi = RouterRpcServer.getRemoteUser();
    final Method m = remoteMethod.getMethod();
    List<IOException> thrownExceptions = new ArrayList<>();
    List<Object> results = new ArrayList<>();
    CompletableFuture<Object[]> completableFuture =
        CompletableFuture.completedFuture(new Object[] {null, false});
    // Invoke in priority order
    for (final RemoteLocationContext loc : locations) {
      String ns = loc.getNameserviceId();
      acquirePermit(ns, ugi, remoteMethod, controller);
      completableFuture = completableFuture.thenCompose(args -> {
        boolean complete = (boolean) args[1];
        if (complete) {
          return CompletableFuture.completedFuture(new Object[]{args[0], true});
        }
        return invokeSequentialToOneNs(ugi, m,
            thrownExceptions, remoteMethod, loc, expectedResultClass,
            expectedResultValue, results);
      });

      releasePermit(ns, ugi, remoteMethod, controller);
    }

    CompletableFuture<Object> resultFuture = completableFuture.thenApply(args -> {
      boolean complete = (boolean) args[1];
      if (complete) {
        return args[0];
      }
      if (!thrownExceptions.isEmpty()) {
        // An unavailable subcluster may be the actual cause
        // We cannot surface other exceptions (e.g., FileNotFoundException)
        for (int i = 0; i < thrownExceptions.size(); i++) {
          IOException ioe = thrownExceptions.get(i);
          if (isUnavailableException(ioe)) {
            throw new CompletionException(ioe);
          }
        }

        // re-throw the first exception thrown for compatibility
        throw new CompletionException(thrownExceptions.get(0));
      }
      // Return the first result, whether it is the value or not
      return new RemoteResult<>(locations.get(0), results.get(0));
    });
    CUR_COMPLETABLE_FUTURE.set(resultFuture);
//    return (RemoteResult) getResult();
    return null;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private CompletableFuture<Object[]> invokeSequentialToOneNs(
      final UserGroupInformation ugi, final Method m,
      final List<IOException> thrownExceptions,
      final RemoteMethod remoteMethod, final RemoteLocationContext loc,
      final Class expectedResultClass, final Object expectedResultValue,
      final List<Object> results) {
    String ns = loc.getNameserviceId();
    boolean isObserverRead = isObserverReadEligible(ns, m);
    try {
      List<? extends FederationNamenodeContext> namenodes =
          getOrderedNamenodes(ns, isObserverRead);
      Class<?> proto = remoteMethod.getProtocol();
      Object[] params = remoteMethod.getParams(loc);

      CompletableFuture<Object> completableFuture =
          (CompletableFuture<Object>) invokeMethod(ugi, namenodes,
              isObserverRead, proto, m, params);
      return completableFuture.handle((result, e) -> {
        if (e == null) {
          // Check if the result is what we expected
          if (isExpectedClass(expectedResultClass, result) &&
              isExpectedValue(expectedResultValue, result)) {
            // Valid result, stop here
            return new Object[] {new RemoteResult<>(loc, result), true};
          } else {
            results.add(result);
            return new Object[] {null, false};
          }
        }

        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          IOException ioe = (IOException) cause;
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
        return new Object[] {null, false};
      });
    }catch (IOException ioe) {
      throw new CompletionException(ioe);
    }
  }

  @Override
  public <T extends RemoteLocationContext, R> Map<T, R> invokeConcurrent(
      final Collection<T> locations, final RemoteMethod method,
      boolean requireResponse, boolean standby, long timeOutMs, Class<R> clazz)
      throws IOException {
    invokeConcurrentAsync(locations, method, standby, timeOutMs, clazz);
    CompletableFuture<Object> completableFuture = CUR_COMPLETABLE_FUTURE.get();
    completableFuture =  completableFuture.thenApply(o -> {
      final List<RemoteResult<T, R>> results = (List<RemoteResult<T, R>>) o;
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
            throw new CompletionException(firstUnavailableException);
          } else {
            throw new CompletionException(thrownExceptions.get(0));
          }
        }
      }
      return ret;
    });
    CUR_COMPLETABLE_FUTURE.set(completableFuture);
//    return (Map<T, R>) getResult();
    return null;
  }

  @Override
  @SuppressWarnings("checkstyle:MethodLength")
  public <T extends RemoteLocationContext, R> List<RemoteResult<T, R>>
      invokeConcurrent(final Collection<T> locations,
                   final RemoteMethod method, boolean standby, long timeOutMs,
                   Class<R> clazz) throws IOException {
    invokeConcurrentAsync(locations, method, standby, timeOutMs, clazz);
//    return (List<RemoteResult<T, R>>) getResult();
    return null;
  }

  private  <T extends RemoteLocationContext, R> List<RemoteResult<T, R>>
      invokeConcurrentAsync(final Collection<T> locations,
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
      boolean isObserverRead = isObserverReadEligible(ns, m);
      final List<? extends FederationNamenodeContext> namenodes =
          getOrderedNamenodes(ns, isObserverRead);
      RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
      acquirePermit(ns, ugi, method, controller);
      try {
        Class<?> proto = method.getProtocol();
        Object[] paramList = method.getParams(location);
        invokeMethod(ugi, namenodes, isObserverRead, proto, m, paramList);
        CompletableFuture<Object> completableFuture = CUR_COMPLETABLE_FUTURE.get();
        completableFuture = completableFuture.exceptionally(e -> {
          IOException ioe = (IOException) e.getCause();
          throw new CompletionException(processException(ioe, location));
        }).thenApply(result -> {
          RemoteResult<T, R> remoteResult =
              (RemoteResult<T, R>) new RemoteResult<>(location, result);
          return Collections.singletonList(remoteResult);
        });
        CUR_COMPLETABLE_FUTURE.set(completableFuture);
        return null;
      } catch (IOException ioe) {
        // Localize the exception
        throw processException(ioe, location);
      } finally {
        releasePermit(ns, ugi, method, controller);
      }
    }

    if (rpcMonitor != null) {
      rpcMonitor.incrProcessingOp();
    }
    if (this.router.getRouterClientMetrics() != null) {
      this.router.getRouterClientMetrics().incInvokedConcurrent(m);
    }

    RouterRpcFairnessPolicyController controller = getRouterRpcFairnessPolicyController();
    acquirePermit(CONCURRENT_NS, ugi, method, controller);

    List<T> orderedLocations = new ArrayList<>();
    List<CompletableFuture<Object>> completableFutures = new ArrayList<>();
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
          final List<FederationNamenodeContext> nnList =
              Collections.singletonList(nn);
          String nnId = nn.getNamenodeId();
          T nnLocation = location;
          if (location instanceof RemoteLocation) {
            nnLocation = (T)new RemoteLocation(nsId, nnId, location.getDest());
          }
          orderedLocations.add(nnLocation);
          invokeMethod(ugi, nnList, isObserverRead, proto, m, paramList);
          completableFutures.add(CUR_COMPLETABLE_FUTURE.get());
        }
      } else {
        // Call the objectGetter in order of nameservices in the NS list
        orderedLocations.add(location);
        invokeMethod(ugi, namenodes, isObserverRead, proto, m, paramList);
        completableFutures.add(CUR_COMPLETABLE_FUTURE.get());
      }
    }

    CompletableFuture<Object>[] completableFuturesArray =
        new CompletableFuture[completableFutures.size()];
    completableFuturesArray = completableFutures.toArray(completableFuturesArray);
    CompletableFuture<Void> allFuture = CompletableFuture.allOf(completableFuturesArray);
    CompletableFuture<Object> resultCompletable = allFuture.handle((unused, throwable) -> {
      List<RemoteResult<T, R>> results = new ArrayList<>();
      for (int i=0; i<completableFutures.size(); i++) {
        T location = orderedLocations.get(i);
        CompletableFuture<Object> resultFuture = completableFutures.get(i);
        Object result = null;
        try {
          result = resultFuture.get();
          results.add((RemoteResult<T, R>) new RemoteResult<>(location, result));
        } catch (InterruptedException ignored) {
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          IOException ioe = null;
          if (cause instanceof CancellationException) {
            T loc = orderedLocations.get(i);
            String msg = "Invocation to \"" + loc + "\" for \""
                + method.getMethodName() + "\" timed out";
            LOG.error(msg);
            ioe = new SubClusterTimeoutException(msg);
          } else if (cause instanceof IOException) {
            LOG.debug("Cannot execute {} in {}: {}",
                m.getName(), location, cause.getMessage());
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
    });

    CUR_COMPLETABLE_FUTURE.set(resultCompletable);
    releasePermit(CONCURRENT_NS, ugi, method, controller);
    return null;
  }

  @Override
  public <T> T invokeSingle(final RemoteLocationContext location,
                            RemoteMethod remoteMethod, Class<T> clazz) throws IOException {
    List<RemoteLocationContext> locations = Collections.singletonList(location);
    invokeSequential(locations, remoteMethod);
//    return (T) getResult();
    return RouterAsyncRpcUtil.asyncReturn(clazz);
  }

  @Override
  public Object invokeSingle(final String nsId, RemoteMethod method)
      throws IOException {
    super.invokeSingle(nsId, method);
//    return getResult();
    return null;
  }
}