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
package org.apache.hadoop.yarn.server.resourcemanager.federation;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Class to define client method,params and arguments.
 */
public class FederationClientMethod<R> {

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationClientMethod.class);

  /**
   * List of parameters: static and dynamic values, matchings types.
   */
  private final Object[] params;

  /**
   * List of method parameters types, matches parameters.
   */
  private final Class<?>[] types;

  /**
   * String name of the method.
   */
  private final String methodName;

  private FederationStateStore stateStoreClient = null;

  private Clock clock = null;

  private Class<R> clazz;

  public FederationClientMethod(String method, Class<?>[] pTypes, Object... pParams)
      throws YarnException {
    if (pParams.length != pTypes.length) {
      throw new YarnException("Invalid parameters for method " + method);
    }

    this.params = pParams;
    this.types = Arrays.copyOf(pTypes, pTypes.length);
    this.methodName = method;
  }

  public FederationClientMethod(String method, Class pTypes, Object pParams)
      throws YarnException {
    this(method, new Class[]{pTypes}, new Object[]{pParams});
  }

  public FederationClientMethod(String method, Class pTypes, Object pParams, Class<R> rTypes,
      FederationStateStore fedStateStore, Clock fedClock) throws YarnException {
    this(method, pTypes, pParams);
    this.stateStoreClient = fedStateStore;
    this.clock = fedClock;
    this.clazz = rTypes;
  }

  public Object[] getParams() {
    return Arrays.copyOf(this.params, this.params.length);
  }

  public String getMethodName() {
    return methodName;
  }

  /**
   * Get the calling types for this method.
   *
   * @return An array of calling types.
   */
  public Class<?>[] getTypes() {
    return Arrays.copyOf(this.types, this.types.length);
  }

  /**
   * We will use the invoke method to call the method in FederationStateStoreService.
   *
   * @return The result returned after calling the interface.
   * @throws YarnException yarn exception.
   */
  protected R invoke() throws YarnException {
    try {
      long startTime = clock.getTime();
      Method method = FederationStateStore.class.getMethod(methodName, types);
      R result = clazz.cast(method.invoke(stateStoreClient, params));

      long stopTime = clock.getTime();
      FederationStateStoreServiceMetrics.succeededStateStoreServiceCall(
          methodName, stopTime - startTime);
      return result;
    } catch (Exception e) {
      LOG.error("stateStoreClient call method {} error.", methodName, e);
      FederationStateStoreServiceMetrics.failedStateStoreServiceCall(methodName);
      throw new YarnException(e);
    }
  }
}