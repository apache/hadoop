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
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.router.clientrm.ClientMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Class to define admin method,params and arguments.
 */
public class AdminMethod {

  private static final Logger LOG = LoggerFactory.getLogger(AdminMethod.class);

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

  public AdminMethod(String method, Class<?>[] pTypes, Object... pParams)
      throws IOException {
    if (pParams.length != pTypes.length) {
      throw new IOException("Invalid parameters for method " + method);
    }

    this.params = pParams;
    this.types = Arrays.copyOf(pTypes, pTypes.length);
    this.methodName = method;
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
}
