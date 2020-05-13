/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.MockNamenode;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


import static java.util.Arrays.asList;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.getStateStoreConfiguration;
import static org.junit.Assert.assertEquals;

/**
 * Test the scheme of Http address of Namenodes displayed in Router.
 * This feature is managed by {@link DFSConfigKeys#DFS_HTTP_POLICY_KEY}
 */
public class TestRouterNamenodeWebScheme {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterNamenodeWebScheme.class);

  /** Router for the test. */
  private Router router;
  /** Namenodes in the cluster. */
  private Map<String, Map<String, MockNamenode>> nns = new HashMap<>();
  /** Nameservices in the federated cluster. */
  private List<String> nsIds = asList("ns0", "ns1");

  @Before
  public void setup() throws Exception {
    LOG.info("Initialize the Mock Namenodes to monitor");
    for (String nsId : nsIds) {
      nns.put(nsId, new HashMap<>());
      for (String nnId : asList("nn0", "nn1")) {
        nns.get(nsId).put(nnId, new MockNamenode(nsId));
      }
    }

    LOG.info("Set nn0 to active for all nameservices");
    for (Map<String, MockNamenode> nnNS : nns.values()) {
      nnNS.get("nn0").transitionToActive();
      nnNS.get("nn1").transitionToStandby();
    }
  }

  @After
  public void cleanup() throws Exception {
    for (Map<String, MockNamenode> nnNS : nns.values()) {
      for (MockNamenode nn : nnNS.values()) {
        nn.stop();
      }
    }
    nns.clear();

    if (router != null) {
      router.stop();
    }
  }

  /**
   * Get the configuration of the cluster which contains all the Namenodes and
   * their addresses.
   * @return Configuration containing all the Namenodes.
   */
  private Configuration getNamenodesConfig() {
    final Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES,
        StringUtils.join(",", nns.keySet()));
    for (String nsId : nns.keySet()) {
      Set<String> nnIds = nns.get(nsId).keySet();

      StringBuilder sb = new StringBuilder();
      sb.append(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX);
      sb.append(".").append(nsId);
      conf.set(sb.toString(), StringUtils.join(",", nnIds));

      for (String nnId : nnIds) {
        final MockNamenode nn = nns.get(nsId).get(nnId);

        sb = new StringBuilder();
        sb.append(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
        sb.append(".").append(nsId);
        sb.append(".").append(nnId);
        conf.set(sb.toString(), "localhost:" + nn.getRPCPort());

        sb = new StringBuilder();
        sb.append(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
        sb.append(".").append(nsId);
        sb.append(".").append(nnId);
        conf.set(sb.toString(), "localhost:" + nn.getHTTPPort());
      }
    }
    return conf;
  }

  @Test
  public void testWebSchemeHttp() throws IOException {
    testWebScheme(HttpConfig.Policy.HTTP_ONLY, "http");
  }

  @Test
  public void testWebSchemeHttps() throws IOException {
    testWebScheme(HttpConfig.Policy.HTTPS_ONLY, "https");
  }

  private void testWebScheme(HttpConfig.Policy httpPolicy,
      String expectedScheme) throws IOException {
    Configuration nsConf = getNamenodesConfig();

    // Setup the State Store for the Router to use
    Configuration stateStoreConfig = getStateStoreConfiguration();
    stateStoreConfig.setClass(
        RBFConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        MembershipNamenodeResolver.class, ActiveNamenodeResolver.class);
    stateStoreConfig.setClass(
        RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        MountTableResolver.class, FileSubclusterResolver.class);

    Configuration routerConf = new RouterConfigBuilder(nsConf)
        .enableLocalHeartbeat(true)
        .heartbeat()
        .stateStore()
        .rpc()
        .build();

    // set "dfs.http.policy" to "HTTPS_ONLY"
    routerConf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, httpPolicy.name());

    // Specify namenodes (ns1.nn0,ns1.nn1) to monitor
    routerConf.set(RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY, "0.0.0.0:0");
    routerConf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE,
        "ns1.nn0,ns1.nn1");
    routerConf.addResource(stateStoreConfig);

    // Specify local node (ns0.nn1) to monitor
    routerConf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, "ns0");
    routerConf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");

    // Start the Router with the namenodes to monitor
    router = new Router();
    router.init(routerConf);
    router.start();

    // Manually trigger the heartbeat and update the values
    Collection<NamenodeHeartbeatService> heartbeatServices =
        router.getNamenodeHeartbeatServices();
    for (NamenodeHeartbeatService service : heartbeatServices) {
      service.periodicInvoke();
    }
    MembershipNamenodeResolver resolver =
        (MembershipNamenodeResolver) router.getNamenodeResolver();
    resolver.loadCache(true);

    // Check that the webSchemes are "https"
    final List<FederationNamenodeContext> namespaceInfo = new ArrayList<>();
    for (String nsId : nns.keySet()) {
      List<? extends FederationNamenodeContext> nnReports =
          resolver.getNamenodesForNameserviceId(nsId);
      namespaceInfo.addAll(nnReports);
    }
    for (FederationNamenodeContext nnInfo : namespaceInfo) {
      assertEquals("Unexpected scheme for Policy: " + httpPolicy.name(),
          nnInfo.getWebScheme(), expectedScheme);
    }
  }
}