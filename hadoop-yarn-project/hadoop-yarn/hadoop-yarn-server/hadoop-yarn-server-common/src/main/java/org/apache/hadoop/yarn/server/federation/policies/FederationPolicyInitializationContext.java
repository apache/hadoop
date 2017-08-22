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

import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;

/**
 * Context to (re)initialize a {@code FederationAMRMProxyPolicy} and {@code
 * FederationRouterPolicy}.
 */
public class FederationPolicyInitializationContext {

  private SubClusterPolicyConfiguration federationPolicyConfiguration;
  private SubClusterResolver federationSubclusterResolver;
  private FederationStateStoreFacade federationStateStoreFacade;
  private SubClusterId homeSubcluster;

  public FederationPolicyInitializationContext() {
    federationPolicyConfiguration = null;
    federationSubclusterResolver = null;
    federationStateStoreFacade = null;
  }

  public FederationPolicyInitializationContext(
      SubClusterPolicyConfiguration policy, SubClusterResolver resolver,
      FederationStateStoreFacade storeFacade, SubClusterId home) {
    this.federationPolicyConfiguration = policy;
    this.federationSubclusterResolver = resolver;
    this.federationStateStoreFacade = storeFacade;
    this.homeSubcluster = home;
  }

  /**
   * Getter for the {@link SubClusterPolicyConfiguration}.
   *
   * @return the {@link SubClusterPolicyConfiguration} to be used for
   *         initialization.
   */
  public SubClusterPolicyConfiguration getSubClusterPolicyConfiguration() {
    return federationPolicyConfiguration;
  }

  /**
   * Setter for the {@link SubClusterPolicyConfiguration}.
   *
   * @param fedPolicyConfiguration the {@link SubClusterPolicyConfiguration} to
   *          be used for initialization.
   */
  public void setSubClusterPolicyConfiguration(
      SubClusterPolicyConfiguration fedPolicyConfiguration) {
    this.federationPolicyConfiguration = fedPolicyConfiguration;
  }

  /**
   * Getter for the {@link SubClusterResolver}.
   *
   * @return the {@link SubClusterResolver} to be used for initialization.
   */
  public SubClusterResolver getFederationSubclusterResolver() {
    return federationSubclusterResolver;
  }

  /**
   * Setter for the {@link SubClusterResolver}.
   *
   * @param federationSubclusterResolver the {@link SubClusterResolver} to be
   *          used for initialization.
   */
  public void setFederationSubclusterResolver(
      SubClusterResolver federationSubclusterResolver) {
    this.federationSubclusterResolver = federationSubclusterResolver;
  }

  /**
   * Getter for the {@link FederationStateStoreFacade}.
   *
   * @return the facade.
   */
  public FederationStateStoreFacade getFederationStateStoreFacade() {
    return federationStateStoreFacade;
  }

  /**
   * Setter for the {@link FederationStateStoreFacade}.
   *
   * @param federationStateStoreFacade the facade.
   */
  public void setFederationStateStoreFacade(
      FederationStateStoreFacade federationStateStoreFacade) {
    this.federationStateStoreFacade = federationStateStoreFacade;
  }

  /**
   * Returns the current home sub-cluster. Useful for default policy behaviors.
   *
   * @return the home sub-cluster.
   */
  public SubClusterId getHomeSubcluster() {
    return homeSubcluster;
  }

  /**
   * Sets in the context the home sub-cluster. Useful for default policy
   * behaviors.
   *
   * @param homeSubcluster value to set.
   */
  public void setHomeSubcluster(SubClusterId homeSubcluster) {
    this.homeSubcluster = homeSubcluster;
  }

}
