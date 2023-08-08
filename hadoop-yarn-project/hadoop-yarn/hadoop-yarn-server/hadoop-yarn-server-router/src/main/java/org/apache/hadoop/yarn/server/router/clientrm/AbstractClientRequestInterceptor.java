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

package org.apache.hadoop.yarn.server.router.clientrm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the {@link ClientRequestInterceptor} interface and provides common
 * functionality which can can be used and/or extended by other concrete
 * interceptor classes.
 *
 */
public abstract class AbstractClientRequestInterceptor
    implements ClientRequestInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractClientRequestInterceptor.class);

  private Configuration conf;
  private ClientRequestInterceptor nextInterceptor;

  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected UserGroupInformation user = null;

  private RouterDelegationTokenSecretManager tokenSecretManager = null;

  /**
   * Sets the {@link ClientRequestInterceptor} in the chain.
   */
  @Override
  public void setNextInterceptor(ClientRequestInterceptor nextInterceptor) {
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
   * Initializes the {@link ClientRequestInterceptor}.
   */
  @Override
  public void init(String userName) {
    this.user = RouterServerUtil.setupUser(userName);
    if (this.nextInterceptor != null) {
      this.nextInterceptor.init(userName);
    }
  }

  /**
   * Disposes the {@link ClientRequestInterceptor}.
   */
  @Override
  public void shutdown() {
    if (this.nextInterceptor != null) {
      this.nextInterceptor.shutdown();
    }
  }

  /**
   * Gets the next {@link ClientRequestInterceptor} in the chain.
   */
  @Override
  public ClientRequestInterceptor getNextInterceptor() {
    return this.nextInterceptor;
  }

  @Override
  public RouterDelegationTokenSecretManager getTokenSecretManager() {
    return tokenSecretManager;
  }

  @Override
  public void setTokenSecretManager(RouterDelegationTokenSecretManager tokenSecretManager) {
    this.tokenSecretManager = tokenSecretManager;
  }
}
