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

package org.apache.hadoop.yarn.server.federation.policies.manager;

import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

/**
 *
 * Implementors need to provide the ability to serliaze a policy and its
 * configuration as a {@link SubClusterPolicyConfiguration}, as well as provide
 * (re)initialization mechanics for the underlying
 * {@link FederationAMRMProxyPolicy} and {@link FederationRouterPolicy}.
 *
 * The serialization aspects are used by admin APIs or a policy engine to store
 * a serialized configuration in the {@code FederationStateStore}, while the
 * getters methods are used to obtain a propertly inizialized policy in the
 * {@code Router} and {@code AMRMProxy} respectively.
 *
 * This interface by design binds together {@link FederationAMRMProxyPolicy} and
 * {@link FederationRouterPolicy} and provide lifecycle support for
 * serialization and deserialization, to reduce configuration mistakes
 * (combining incompatible policies).
 *
 */
public interface FederationPolicyManager {

  /**
   * If the current instance is compatible, this method returns the same
   * instance of {@link FederationAMRMProxyPolicy} reinitialized with the
   * current context, otherwise a new instance initialized with the current
   * context is provided. If the instance is compatible with the current class
   * the implementors should attempt to reinitalize (retaining state). To affect
   * a complete policy reset oldInstance should be null.
   *
   * @param policyContext the current context
   * @param oldInstance the existing (possibly null) instance.
   *
   * @return an updated {@link FederationAMRMProxyPolicy }.
   *
   * @throws FederationPolicyInitializationException if the initialization
   *           cannot be completed properly. The oldInstance should be still
   *           valid in case of failed initialization.
   */
  FederationAMRMProxyPolicy getAMRMPolicy(
      FederationPolicyInitializationContext policyContext,
      FederationAMRMProxyPolicy oldInstance)
      throws FederationPolicyInitializationException;

  /**
   * If the current instance is compatible, this method returns the same
   * instance of {@link FederationRouterPolicy} reinitialized with the current
   * context, otherwise a new instance initialized with the current context is
   * provided. If the instance is compatible with the current class the
   * implementors should attempt to reinitalize (retaining state). To affect a
   * complete policy reset oldInstance shoulb be set to null.
   *
   * @param policyContext the current context
   * @param oldInstance the existing (possibly null) instance.
   *
   * @return an updated {@link FederationRouterPolicy}.
   *
   * @throws FederationPolicyInitializationException if the initalization cannot
   *           be completed properly. The oldInstance should be still valid in
   *           case of failed initialization.
   */
  FederationRouterPolicy getRouterPolicy(
      FederationPolicyInitializationContext policyContext,
      FederationRouterPolicy oldInstance)
      throws FederationPolicyInitializationException;

  /**
   * This method is invoked to derive a {@link SubClusterPolicyConfiguration}.
   * This is to be used when writing a policy object in the federation policy
   * store.
   *
   * @return a valid policy configuration representing this object
   *         parametrization.
   *
   * @throws FederationPolicyInitializationException if the current state cannot
   *           be serialized properly
   */
  SubClusterPolicyConfiguration serializeConf()
      throws FederationPolicyInitializationException;

  /**
   * This method returns the queue this policy is configured for.
   *
   * @return the name of the queue.
   */
  String getQueue();

  /**
   * This methods provides a setter for the queue this policy is specified for.
   *
   * @param queue the name of the queue.
   */
  void setQueue(String queue);

}
