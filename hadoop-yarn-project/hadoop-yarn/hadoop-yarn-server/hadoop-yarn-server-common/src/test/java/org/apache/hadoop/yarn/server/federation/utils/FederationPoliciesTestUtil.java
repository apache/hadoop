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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.resolver.DefaultSubClusterResolverImpl;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;

import java.net.URL;
import java.util.ArrayList;

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
    conf.set(YarnConfiguration.FEDERATION_MACHINE_LIST, url.getPath());
    resolver.setConf(conf);
    resolver.load();
    return resolver;
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
    FederationStateStoreFacade goodFacade = FederationStateStoreFacade
        .getInstance();
    FederationStateStore fss = mock(FederationStateStore.class);
    GetSubClustersInfoResponse response = GetSubClustersInfoResponse
        .newInstance(new ArrayList<>());
    when(fss.getSubClusters(any())).thenReturn(response);
    goodFacade.reinitialize(fss, new Configuration());
    return goodFacade;
  }
}
