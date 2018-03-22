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

import org.apache.hadoop.conf.Configuration;

/**
 * Implements the {@link RMAdminRequestInterceptor} interface and provides
 * common functionality which can can be used and/or extended by other concrete
 * intercepter classes.
 *
 */
public abstract class AbstractRMAdminRequestInterceptor
    implements RMAdminRequestInterceptor {
  private Configuration conf;
  private RMAdminRequestInterceptor nextInterceptor;

  /**
   * Sets the {@link RMAdminRequestInterceptor} in the chain.
   */
  @Override
  public void setNextInterceptor(RMAdminRequestInterceptor nextInterceptor) {
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
   * Initializes the {@link RMAdminRequestInterceptor}.
   */
  @Override
  public void init(String user) {
    if (this.nextInterceptor != null) {
      this.nextInterceptor.init(user);
    }
  }

  /**
   * Disposes the {@link RMAdminRequestInterceptor}.
   */
  @Override
  public void shutdown() {
    if (this.nextInterceptor != null) {
      this.nextInterceptor.shutdown();
    }
  }

  /**
   * Gets the next {@link RMAdminRequestInterceptor} in the chain.
   */
  @Override
  public RMAdminRequestInterceptor getNextInterceptor() {
    return this.nextInterceptor;
  }

}
