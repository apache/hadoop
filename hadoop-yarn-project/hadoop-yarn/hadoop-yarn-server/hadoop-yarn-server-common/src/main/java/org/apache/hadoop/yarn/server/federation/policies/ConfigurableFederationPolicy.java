/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies;

import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;

/**
 * This interface provides a general method to reinitialize a policy. The
 * semantics are try-n-swap, so in case of an exception is thrown the
 * implmentation must ensure the previous state and configuration is preserved.
 */
public interface ConfigurableFederationPolicy {

  /**
   * This method is invoked to initialize of update the configuration of
   * policies. The implementor should provide try-n-swap semantics, and retain
   * state if possible.
   *
   * @param policyContext the new context to provide to implementor.
   *
   * @throws FederationPolicyInitializationException in case the initialization
   *           fails.
   */
  void reinitialize(FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException;
}
