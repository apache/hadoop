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

package org.apache.hadoop.yarn.server.federation.utils;

import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;


public abstract class FederationMethodWrapper {

  /**
   * List of parameters: static and dynamic values, matching types.
   */
  private Object[] params;

  /**
   * List of method parameters types, matches parameters.
   */
  private Class<?>[] types;

  /**
   * String name of the method.
   */
  private String methodName;

  public FederationMethodWrapper(Class<?>[] pTypes, Object... pParams)
      throws IOException {
    if (pParams.length != pTypes.length) {
      throw new IOException("Invalid parameters for method.");
    }
    this.params = pParams;
    this.types = Arrays.copyOf(pTypes, pTypes.length);
  }

  public Object[] getParams() {
    return Arrays.copyOf(this.params, this.params.length);
  }

  public String getMethodName() {
    return methodName;
  }

  public void setMethodName(String methodName) {
    this.methodName = methodName;
  }

  /**
   * Get the calling types for this method.
   *
   * @return An array of calling types.
   */
  public Class<?>[] getTypes() {
    return Arrays.copyOf(this.types, this.types.length);
  }

  protected abstract <R> Collection<R> invokeConcurrent(Class<R> clazz) throws YarnException;
}
