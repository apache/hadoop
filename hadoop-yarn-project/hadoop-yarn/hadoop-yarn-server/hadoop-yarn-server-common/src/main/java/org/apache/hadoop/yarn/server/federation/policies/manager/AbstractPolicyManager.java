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

import org.apache.hadoop.yarn.server.federation.policies.ConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * This class provides basic implementation for common methods that multiple
 * policies will need to implement.
 */
public abstract class AbstractPolicyManager implements
    FederationPolicyManager {

  private String queue;
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected Class routerFederationPolicy;
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected Class amrmProxyFederationPolicy;

  public static final Logger LOG =
      LoggerFactory.getLogger(AbstractPolicyManager.class);
  /**
   * This default implementation validates the
   * {@link FederationPolicyInitializationContext},
   * then checks whether it needs to reinstantiate the class (null or
   * mismatching type), and reinitialize the policy.
   *
   * @param federationPolicyContext the current context
   * @param oldInstance             the existing (possibly null) instance.
   *
   * @return a valid and fully reinitalized {@link FederationAMRMProxyPolicy}
   * instance
   *
   * @throws FederationPolicyInitializationException if the reinitalization is
   *                                                 not valid, and ensure
   *                                                 previous state is preserved
   */
  public FederationAMRMProxyPolicy getAMRMPolicy(
      FederationPolicyInitializationContext federationPolicyContext,
      FederationAMRMProxyPolicy oldInstance)
      throws FederationPolicyInitializationException {

    if (amrmProxyFederationPolicy == null) {
      throw new FederationPolicyInitializationException("The parameter "
          + "amrmProxyFederationPolicy should be initialized in "
          + this.getClass().getSimpleName() + " constructor.");
    }

    try {
      return (FederationAMRMProxyPolicy) internalPolicyGetter(
          federationPolicyContext, oldInstance, amrmProxyFederationPolicy);
    } catch (ClassCastException e) {
      throw new FederationPolicyInitializationException(e);
    }

  }

  /**
   * This default implementation validates the
   * {@link FederationPolicyInitializationContext},
   * then checks whether it needs to reinstantiate the class (null or
   * mismatching type), and reinitialize the policy.
   *
   * @param federationPolicyContext the current context
   * @param oldInstance             the existing (possibly null) instance.
   *
   * @return a valid and fully reinitalized {@link FederationRouterPolicy}
   * instance
   *
   * @throws FederationPolicyInitializationException if the reinitalization is
   *                                                 not valid, and ensure
   *                                                 previous state is preserved
   */

  public FederationRouterPolicy getRouterPolicy(
      FederationPolicyInitializationContext federationPolicyContext,
      FederationRouterPolicy oldInstance)
      throws FederationPolicyInitializationException {

    //checks that sub-types properly initialize the types of policies
    if (routerFederationPolicy == null) {
      throw new FederationPolicyInitializationException("The policy "
          + "type should be initialized in " + this.getClass().getSimpleName()
          + " constructor.");
    }

    try {
      return (FederationRouterPolicy) internalPolicyGetter(
          federationPolicyContext, oldInstance, routerFederationPolicy);
    } catch (ClassCastException e) {
      throw new FederationPolicyInitializationException(e);
    }
  }

  @Override
  public SubClusterPolicyConfiguration serializeConf()
      throws FederationPolicyInitializationException {
    // default implementation works only for sub-classes which do not require
    // any parameters
    ByteBuffer buf = ByteBuffer.allocate(0);
    return SubClusterPolicyConfiguration
        .newInstance(getQueue(), this.getClass().getCanonicalName(), buf);
  }

  @Override
  public String getQueue() {
    return queue;
  }

  @Override
  public void setQueue(String queue) {
    this.queue = queue;
  }

  /**
   * Common functionality to instantiate a reinitialize a {@link
   * ConfigurableFederationPolicy}.
   */
  private ConfigurableFederationPolicy internalPolicyGetter(
      final FederationPolicyInitializationContext federationPolicyContext,
      ConfigurableFederationPolicy oldInstance, Class policy)
      throws FederationPolicyInitializationException {

    FederationPolicyInitializationContextValidator
        .validate(federationPolicyContext, this.getClass().getCanonicalName());

    if (oldInstance == null || !oldInstance.getClass().equals(policy)) {
      try {
        oldInstance = (ConfigurableFederationPolicy) policy.newInstance();
      } catch (InstantiationException e) {
        throw new FederationPolicyInitializationException(e);
      } catch (IllegalAccessException e) {
        throw new FederationPolicyInitializationException(e);
      }
    }

    //copying the context to avoid side-effects
    FederationPolicyInitializationContext modifiedContext =
        updateContext(federationPolicyContext,
            oldInstance.getClass().getCanonicalName());

    oldInstance.reinitialize(modifiedContext);
    return oldInstance;
  }

  /**
   * This method is used to copy-on-write the context, that will be passed
   * downstream to the router/amrmproxy policies.
   */
  private FederationPolicyInitializationContext updateContext(
      FederationPolicyInitializationContext federationPolicyContext,
      String type) {
    // copying configuration and context to avoid modification of original
    SubClusterPolicyConfiguration newConf = SubClusterPolicyConfiguration
        .newInstance(federationPolicyContext
            .getSubClusterPolicyConfiguration());
    newConf.setType(type);

    return new FederationPolicyInitializationContext(newConf,
                  federationPolicyContext.getFederationSubclusterResolver(),
                  federationPolicyContext.getFederationStateStoreFacade(),
                  federationPolicyContext.getHomeSubcluster());
  }

}
