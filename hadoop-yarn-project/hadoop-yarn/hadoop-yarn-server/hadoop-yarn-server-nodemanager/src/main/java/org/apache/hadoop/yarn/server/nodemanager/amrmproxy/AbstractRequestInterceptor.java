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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

/**
 * Implements the RequestInterceptor interface and provides common functionality
 * which can can be used and/or extended by other concrete intercepter classes.
 *
 */
public abstract class AbstractRequestInterceptor implements
    RequestInterceptor {
  private Configuration conf;
  private AMRMProxyApplicationContext appContext;
  private RequestInterceptor nextInterceptor;

  /**
   * Sets the {@link RequestInterceptor} in the chain.
   */
  @Override
  public void setNextInterceptor(RequestInterceptor nextInterceptor) {
    this.nextInterceptor = nextInterceptor;
  }

  /**
   * Sets the {@link Configuration}.
   */

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (this.nextInterceptor != null) {
      this.nextInterceptor.setConf(conf);
    }
  }

  /**
   * Gets the {@link Configuration}.
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Initializes the {@link RequestInterceptor}.
   */
  @Override
  public void init(AMRMProxyApplicationContext appContext) {
    Preconditions.checkState(this.appContext == null,
        "init is called multiple times on this interceptor: "
            + this.getClass().getName());
    this.appContext = appContext;
    if (this.nextInterceptor != null) {
      this.nextInterceptor.init(appContext);
    }
  }

  /**
   * Disposes the {@link RequestInterceptor}.
   */
  @Override
  public void shutdown() {
    if (this.nextInterceptor != null) {
      this.nextInterceptor.shutdown();
    }
  }

  /**
   * Gets the next {@link RequestInterceptor} in the chain.
   */
  @Override
  public RequestInterceptor getNextInterceptor() {
    return this.nextInterceptor;
  }

  /**
   * Gets the {@link AMRMProxyApplicationContext}.
   */
  public AMRMProxyApplicationContext getApplicationContext() {
    return this.appContext;
  }
}
