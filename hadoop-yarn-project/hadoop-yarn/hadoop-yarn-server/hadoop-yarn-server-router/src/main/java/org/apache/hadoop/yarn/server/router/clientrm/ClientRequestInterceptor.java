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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;

/**
 * Defines the contract to be implemented by the request interceptor classes,
 * that can be used to intercept and inspect messages sent from the client to
 * the resource manager.
 */
public interface ClientRequestInterceptor
    extends ApplicationClientProtocol, Configurable {
  /**
   * This method is called for initializing the interceptor. This is guaranteed
   * to be called only once in the lifetime of this instance.
   *
   * @param user the name of the client
   */
  void init(String user);

  /**
   * This method is called to release the resources held by the interceptor.
   * This will be called when the application pipeline is being destroyed. The
   * concrete implementations should dispose the resources and forward the
   * request to the next interceptor, if any.
   */
  void shutdown();

  /**
   * Sets the next interceptor in the pipeline. The concrete implementation of
   * this interface should always pass the request to the nextInterceptor after
   * inspecting the message. The last interceptor in the chain is responsible to
   * send the messages to the resource manager service and so the last
   * interceptor will not receive this method call.
   *
   * @param nextInterceptor the ClientRequestInterceptor to set in the pipeline
   */
  void setNextInterceptor(ClientRequestInterceptor nextInterceptor);

  /**
   * Returns the next interceptor in the chain.
   *
   * @return the next interceptor in the chain
   */
  ClientRequestInterceptor getNextInterceptor();

  /**
   * Set RouterDelegationTokenSecretManager for specific interceptor to support Token operations,
   * including create Token, update Token, and delete Token.
   *
   * @param tokenSecretManager Router DelegationTokenSecretManager
   */
  void setTokenSecretManager(RouterDelegationTokenSecretManager tokenSecretManager);

  /**
   * Get RouterDelegationTokenSecretManager.
   *
   * @return Router DelegationTokenSecretManager.
   */
  RouterDelegationTokenSecretManager getTokenSecretManager();
}
