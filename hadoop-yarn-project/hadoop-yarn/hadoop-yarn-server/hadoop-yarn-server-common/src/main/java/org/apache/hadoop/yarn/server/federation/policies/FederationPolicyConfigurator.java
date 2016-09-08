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

import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;


import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;

import org.apache.hadoop.yarn.server.federation.policies.router
    .FederationRouterPolicy;

/**
 * Implementors of this interface are capable to instantiate and (re)initalize
 * {@link FederationAMRMProxyPolicy} and {@link FederationRouterPolicy} based on
 * a {@link FederationPolicyInitializationContext}. The reason to bind these two
 * policies together is to make sure we remain consistent across the router and
 * amrmproxy policy decisions.
 */
public interface FederationPolicyConfigurator {

  /**
   * If the current instance is compatible, this method returns the same
   * instance of {@link FederationAMRMProxyPolicy} reinitialized with the
   * current context, otherwise a new instance initialized with the current
   * context is provided. If the instance is compatible with the current class
   * the implementors should attempt to reinitalize (retaining state). To affect
   * a complete policy reset oldInstance should be null.
   *
   * @param federationPolicyInitializationContext the current context
   * @param oldInstance                           the existing (possibly null)
   *                                              instance.
   *
   * @return an updated {@link FederationAMRMProxyPolicy
  }.
   *
   * @throws FederationPolicyInitializationException if the initialization
   *                                                 cannot be completed
   *                                                 properly. The oldInstance
   *                                                 should be still valid in
   *                                                 case of failed
   *                                                 initialization.
   */
  FederationAMRMProxyPolicy getAMRMPolicy(
      FederationPolicyInitializationContext
          federationPolicyInitializationContext,
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
   * @param federationPolicyInitializationContext the current context
   * @param oldInstance                           the existing (possibly null)
   *                                              instance.
   *
   * @return an updated {@link FederationRouterPolicy}.
   *
   * @throws FederationPolicyInitializationException if the initalization cannot
   *                                                 be completed properly. The
   *                                                 oldInstance should be still
   *                                                 valid in case of failed
   *                                                 initialization.
   */
  FederationRouterPolicy getRouterPolicy(
      FederationPolicyInitializationContext
          federationPolicyInitializationContext,
      FederationRouterPolicy oldInstance)
      throws FederationPolicyInitializationException;

}
