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
package org.apache.hadoop.yarn.server.router.rmadmin;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationMethodWrapper;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Class to define admin method, params and arguments.
 */
public class RMAdminProtocolMethod extends FederationMethodWrapper {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMAdminProtocolMethod.class);

  private FederationStateStoreFacade federationFacade;
  private FederationRMAdminInterceptor rmAdminInterceptor;
  private Configuration configuration;

  public RMAdminProtocolMethod(Class<?>[] pTypes, Object... pParams)
      throws IOException {
    super(pTypes, pParams);
  }

  public <R> Collection<R> invokeConcurrent(FederationRMAdminInterceptor interceptor,
      Class<R> clazz, String subClusterId) throws YarnException {
    this.rmAdminInterceptor = interceptor;
    this.federationFacade = FederationStateStoreFacade.getInstance(interceptor.getConf());
    this.configuration = interceptor.getConf();
    if (StringUtils.isNotBlank(subClusterId)) {
      return invoke(clazz, subClusterId);
    } else {
      return invokeConcurrent(clazz);
    }
  }

  @Override
  protected <R> Collection<R> invokeConcurrent(Class<R> clazz) throws YarnException {
    String methodName = Thread.currentThread().getStackTrace()[3].getMethodName();
    this.setMethodName(methodName);

    ThreadPoolExecutor executorService = rmAdminInterceptor.getExecutorService();

    // Get Active SubClusters
    Map<SubClusterId, SubClusterInfo> subClusterInfo =
        federationFacade.getSubClusters(true);
    Collection<SubClusterId> subClusterIds = subClusterInfo.keySet();

    List<Callable<Pair<SubClusterId, Object>>> callables = new ArrayList<>();
    List<Future<Pair<SubClusterId, Object>>> futures = new ArrayList<>();
    Map<SubClusterId, Exception> exceptions = new TreeMap<>();

    // Generate parallel Callable tasks
    for (SubClusterId subClusterId : subClusterIds) {
      callables.add(() -> {
        ResourceManagerAdministrationProtocol protocol =
            rmAdminInterceptor.getAdminRMProxyForSubCluster(subClusterId);
        Class<?>[] types = this.getTypes();
        Object[] params = this.getParams();
        Method method = ResourceManagerAdministrationProtocol.class.getMethod(methodName, types);
        Object result = method.invoke(protocol, params);
        return Pair.of(subClusterId, result);
      });
    }

    // Get results from multiple threads
    Map<SubClusterId, R> results = new TreeMap<>();
    try {
      futures.addAll(executorService.invokeAll(callables));
      futures.stream().forEach(future -> {
        SubClusterId subClusterId = null;
        try {
          Pair<SubClusterId, Object> pair = future.get();
          subClusterId = pair.getKey();
          Object result = pair.getValue();
          if (result != null) {
            R rResult = clazz.cast(result);
            results.put(subClusterId, rResult);
          }
        } catch (InterruptedException | ExecutionException e) {
          Throwable cause = e.getCause();
          LOG.error("Cannot execute {} on {}: {}", methodName, subClusterId, cause.getMessage());
          exceptions.put(subClusterId, e);
        }
      });
    } catch (InterruptedException e) {
      throw new YarnException("invokeConcurrent Failed.", e);
    }

    // All sub-clusters return results to be considered successful,
    // otherwise an exception will be thrown.
    if (exceptions != null && !exceptions.isEmpty()) {
      Set<SubClusterId> subClusterIdSets = exceptions.keySet();
      throw new YarnException("invokeConcurrent Failed, An exception occurred in subClusterIds = " +
          StringUtils.join(subClusterIdSets, ","));
    }

    // return result
    return results.values();
  }

  /**
   * Call the method in the protocol according to the subClusterId.
   *
   * @param clazz return type
   * @param subClusterId subCluster Id
   * @param <R> Generic R
   * @return response collection.
   * @throws YarnException yarn exception.
   */
  protected <R> Collection<R> invoke(Class<R> clazz, String subClusterId) throws YarnException {

    // Get the method name to call
    String methodName = Thread.currentThread().getStackTrace()[3].getMethodName();
    this.setMethodName(methodName);

    // Get Active SubClusters
    Map<SubClusterId, SubClusterInfo> subClusterInfoMap =
        federationFacade.getSubClusters(true);

    // According to subCluster of string type, convert to SubClusterId type
    SubClusterId subClusterIdKey = SubClusterId.newInstance(subClusterId);

    // If the provided subCluster is not Active or does not exist,
    // an exception will be returned directly.
    if (!subClusterInfoMap.containsKey(subClusterIdKey)) {
      throw new YarnException("subClusterId = " + subClusterId + " is not an active subCluster.");
    }

    // Call the method in the protocol and convert it according to clazz.
    try {
      ResourceManagerAdministrationProtocol protocol =
          rmAdminInterceptor.getAdminRMProxyForSubCluster(subClusterIdKey);
      Class<?>[] types = this.getTypes();
      Object[] params = this.getParams();
      Method method = ResourceManagerAdministrationProtocol.class.getMethod(methodName, types);
      Object result = method.invoke(protocol, params);
      if (result != null) {
        return Collections.singletonList(clazz.cast(result));
      }
    } catch (Exception e) {
      throw new YarnException("invoke Failed, An exception occurred in subClusterId = " +
          subClusterId, e);
    }
    throw new YarnException("invoke Failed, An exception occurred in subClusterId = " +
        subClusterId);
  }
}
