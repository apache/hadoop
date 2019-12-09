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
package org.apache.hadoop.yarn.server.federation.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.ConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.resolver.DefaultSubClusterResolverImpl;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.*;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Support class providing common initialization methods to test federation
 * policies.
 */
public final class FederationPoliciesTestUtil {

  private FederationPoliciesTestUtil() {
    // disabled.
  }

  private static final String FEDR_NODE_PREFIX = "fedr-test-node-";


  public static List<ResourceRequest> createResourceRequests(String[] hosts,
      int memory, int vCores, int priority, int containers,
      String labelExpression, boolean relaxLocality) throws YarnException {
    List<ResourceRequest> reqs = new ArrayList<ResourceRequest>();
    for (String host : hosts) {
      ResourceRequest hostReq =
          createResourceRequest(host, memory, vCores, priority, containers,
              labelExpression, relaxLocality);
      reqs.add(hostReq);
      ResourceRequest rackReq =
          createResourceRequest("/default-rack", memory, vCores, priority,
              containers, labelExpression, relaxLocality);
      reqs.add(rackReq);
    }

    ResourceRequest offRackReq =
        createResourceRequest(ResourceRequest.ANY, memory, vCores, priority,
            containers, labelExpression, relaxLocality);
    reqs.add(offRackReq);
    return reqs;
  }

  protected static ResourceRequest createResourceRequest(String resource,
      int memory, int vCores, int priority, int containers,
      boolean relaxLocality) throws YarnException {
    return createResourceRequest(resource, memory, vCores, priority, containers,
        null, relaxLocality);
  }

  @SuppressWarnings("checkstyle:parameternumber")
  public static ResourceRequest createResourceRequest(long id, String resource,
      int memory, int vCores, int priority, int containers,
      String labelExpression, boolean relaxLocality) throws YarnException {
    ResourceRequest out =
        createResourceRequest(resource, memory, vCores, priority, containers,
            labelExpression, relaxLocality);
    out.setAllocationRequestId(id);
    return out;
  }

  public static ResourceRequest createResourceRequest(String resource,
      int memory, int vCores, int priority, int containers,
      String labelExpression, boolean relaxLocality) throws YarnException {
    ResourceRequest req = Records.newRecord(ResourceRequest.class);
    req.setResourceName(resource);
    req.setNumContainers(containers);
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(priority);
    req.setPriority(pri);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemorySize(memory);
    capability.setVirtualCores(vCores);
    req.setCapability(capability);
    if (labelExpression != null) {
      req.setNodeLabelExpression(labelExpression);
    }
    req.setRelaxLocality(relaxLocality);
    return req;
  }

  public static void initializePolicyContext(
      FederationPolicyInitializationContext fpc, ConfigurableFederationPolicy
      policy, WeightedPolicyInfo policyInfo,
      Map<SubClusterId, SubClusterInfo> activeSubclusters, Configuration conf)
      throws YarnException {
    ByteBuffer buf = policyInfo.toByteBuffer();
    fpc.setSubClusterPolicyConfiguration(SubClusterPolicyConfiguration
        .newInstance("queue1", policy.getClass().getCanonicalName(), buf));
    FederationStateStoreFacade facade = FederationStateStoreFacade
        .getInstance();
    FederationStateStore fss = mock(FederationStateStore.class);

    if (activeSubclusters == null) {
      activeSubclusters = new HashMap<SubClusterId, SubClusterInfo>();
    }
    GetSubClustersInfoResponse response = GetSubClustersInfoResponse
        .newInstance(new ArrayList<SubClusterInfo>(activeSubclusters.values()));

    when(fss.getSubClusters(any())).thenReturn(response);
    facade.reinitialize(fss, conf);
    fpc.setFederationStateStoreFacade(facade);
    policy.reinitialize(fpc);
  }

  public static void initializePolicyContext(
      ConfigurableFederationPolicy policy,
      WeightedPolicyInfo policyInfo,
      Map<SubClusterId, SubClusterInfo> activeSubclusters)
          throws YarnException {
    initializePolicyContext(
        policy, policyInfo, activeSubclusters, "homesubcluster");
  }

  public static void initializePolicyContext(
      ConfigurableFederationPolicy policy,
      WeightedPolicyInfo policyInfo,
      Map<SubClusterId, SubClusterInfo> activeSubclusters,
      String subclusterId) throws YarnException {
    FederationPolicyInitializationContext context =
        new FederationPolicyInitializationContext(null, initResolver(),
            initFacade(), SubClusterId.newInstance(subclusterId));
    initializePolicyContext(context, policy, policyInfo, activeSubclusters,
        new Configuration());
  }

  /**
   * Initialize a {@link SubClusterResolver}.
   *
   * @return a subcluster resolver for tests.
   */
  public static SubClusterResolver initResolver() {
    YarnConfiguration conf = new YarnConfiguration();
    SubClusterResolver resolver =
        new DefaultSubClusterResolverImpl();
    URL url =
        Thread.currentThread().getContextClassLoader().getResource("nodes");
    if (url == null) {
      throw new RuntimeException(
          "Could not find 'nodes' dummy file in classpath");
    }
    // This will get rid of the beginning '/' in the url in Windows env
    File file = new File(url.getPath());

    conf.set(YarnConfiguration.FEDERATION_MACHINE_LIST, file.getPath());
    resolver.setConf(conf);
    resolver.load();
    return resolver;
  }

  /**
   * Initialiaze a main-memory {@link FederationStateStoreFacade} used for
   * testing, wiht a mock resolver.
   *
   * @param subClusterInfos the list of subclusters to be served on
   *                        getSubClusters invocations.
   *
   * @return the facade.
   *
   * @throws YarnException in case the initialization is not successful.
   */

  public static FederationStateStoreFacade initFacade(
      List<SubClusterInfo> subClusterInfos, SubClusterPolicyConfiguration
      policyConfiguration) throws YarnException {
    FederationStateStoreFacade goodFacade = FederationStateStoreFacade
        .getInstance();
    FederationStateStore fss = mock(FederationStateStore.class);
    GetSubClustersInfoResponse response = GetSubClustersInfoResponse
        .newInstance(subClusterInfos);
    when(fss.getSubClusters(any())).thenReturn(response);

    List<SubClusterPolicyConfiguration> configurations = new ArrayList<>();
    configurations.add(policyConfiguration);

    GetSubClusterPoliciesConfigurationsResponse policiesResponse =
        GetSubClusterPoliciesConfigurationsResponse
            .newInstance(configurations);
    when(fss.getPoliciesConfigurations(any())).thenReturn(policiesResponse);

    GetSubClusterPolicyConfigurationResponse policyResponse =
        GetSubClusterPolicyConfigurationResponse
            .newInstance(policyConfiguration);
    when(fss.getPolicyConfiguration(any())).thenReturn(policyResponse);

    goodFacade.reinitialize(fss, new Configuration());
    return goodFacade;
  }

  /**
   * Initialiaze a main-memory {@link FederationStateStoreFacade} used for
   * testing, wiht a mock resolver.
   *
   * @return the facade.
   *
   * @throws YarnException in case the initialization is not successful.
   */
  public static FederationStateStoreFacade initFacade() throws YarnException {
    return initFacade(new ArrayList<>(), mock(SubClusterPolicyConfiguration
        .class));
  }

}
