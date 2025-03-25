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
package org.apache.hadoop.hdfs.server.federation.router;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.io.IOUtils;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HTTP_ENABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterNetworkTopologyServlet.clusterNoDatanodes;
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterNetworkTopologyServlet.clusterWithDatanodes;
import static org.apache.hadoop.hdfs.server.federation.router.TestRouterNetworkTopologyServlet.setUp;

@SuppressWarnings("checkstyle:VisibilityModifier")
public class TestRouterNetworkTopologyServlet {

  public static StateStoreDFSCluster clusterWithDatanodes;
  public static StateStoreDFSCluster clusterNoDatanodes;

  public static final String ASYNC_MODE = "ASYNC";
  public static final String SYNC_MODE = "SYNC";

  public static void setUp(String rpcMode) throws Exception {
    // Builder configuration.
    Configuration routerConf =
        new RouterConfigBuilder().stateStore().admin().quota().rpc().build();
    routerConf.set(DFS_ROUTER_HTTP_ENABLE, "true");
    // Use async router rpc.
    if (rpcMode.equals("ASYNC")) {
      routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    }
    Configuration hdfsConf = new Configuration(false);

    // Build and start a federated cluster.
    clusterWithDatanodes = new StateStoreDFSCluster(false, 2,
        MultipleDestinationMountTableResolver.class);
    clusterWithDatanodes.addNamenodeOverrides(hdfsConf);
    clusterWithDatanodes.addRouterOverrides(routerConf);
    clusterWithDatanodes.setNumDatanodesPerNameservice(9);
    clusterWithDatanodes.setIndependentDNs();
    clusterWithDatanodes.setRacks(
        new String[] {"/rack1", "/rack1", "/rack1", "/rack2", "/rack2",
            "/rack2", "/rack3", "/rack3", "/rack3", "/rack4", "/rack4",
            "/rack4", "/rack5", "/rack5", "/rack5", "/rack6", "/rack6",
            "/rack6"});
    clusterWithDatanodes.startCluster();
    clusterWithDatanodes.startRouters();
    clusterWithDatanodes.waitClusterUp();
    clusterWithDatanodes.waitActiveNamespaces();

    // Build and start a federated cluster.
    clusterNoDatanodes = new StateStoreDFSCluster(false, 2,
        MultipleDestinationMountTableResolver.class);
    clusterNoDatanodes.addNamenodeOverrides(hdfsConf);
    clusterNoDatanodes.addRouterOverrides(routerConf);
    clusterNoDatanodes.setNumDatanodesPerNameservice(0);
    clusterNoDatanodes.setIndependentDNs();
    clusterNoDatanodes.startCluster();
    clusterNoDatanodes.startRouters();
    clusterNoDatanodes.waitClusterUp();
    clusterNoDatanodes.waitActiveNamespaces();
  }

  @Nested
  @ExtendWith(RouterServerHelper.class)
  class TestWithAsyncRouterRpc {

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testPrintTopologyTextFormatAsync(String rpcMode) throws Exception {
      testPrintTopologyTextFormat();
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testPrintTopologyJsonFormatAsync(String rpcMode) throws Exception {
      testPrintTopologyJsonFormat();
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testPrintTopologyNoDatanodesTextFormatAsync(String rpcMode)
        throws Exception {
      testPrintTopologyNoDatanodesTextFormat();
    }

    @ParameterizedTest
    @ValueSource(strings = {ASYNC_MODE})
    public void testPrintTopologyNoDatanodesJsonFormatAsync(String rpcMode)
        throws Exception {
      testPrintTopologyNoDatanodesJsonFormat();
    }
  }

  @Nested
  @ExtendWith(RouterServerHelper.class)
  class TestWithSyncRouterRpc {

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testPrintTopologyTextFormatSync(String rpcMode) throws Exception {
      testPrintTopologyTextFormat();
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testPrintTopologyJsonFormatSync(String rpcMode) throws Exception {
      testPrintTopologyJsonFormat();
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testPrintTopologyNoDatanodesTextFormatSync(String rpcMode)
        throws Exception {
      testPrintTopologyNoDatanodesTextFormat();
    }

    @ParameterizedTest
    @ValueSource(strings = {SYNC_MODE})
    public void testPrintTopologyNoDatanodesJsonFormatSync(String rpcMode)
        throws Exception {
      testPrintTopologyNoDatanodesJsonFormat();
    }
  }

  public void testPrintTopologyTextFormat() throws Exception {
    // Get http Address.
    String httpAddress = clusterWithDatanodes.getRandomRouter().getRouter()
        .getHttpServerAddress().toString();

    // Send http request.
    URL url = new URL("http:/" + httpAddress + "/topology");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setReadTimeout(20000);
    conn.setConnectTimeout(20000);
    conn.connect();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
    StringBuilder sb =
        new StringBuilder("-- Network Topology -- \n");
    sb.append(out);
    sb.append("\n-- Network Topology -- ");
    String topology = sb.toString();

    // Assert rack info.
    assertTrue(topology.contains("/ns0/rack1"));
    assertTrue(topology.contains("/ns0/rack2"));
    assertTrue(topology.contains("/ns0/rack3"));
    assertTrue(topology.contains("/ns1/rack4"));
    assertTrue(topology.contains("/ns1/rack5"));
    assertTrue(topology.contains("/ns1/rack6"));

    // Assert node number.
    assertEquals(18,
        topology.split("127.0.0.1").length - 1);
  }

  public void testPrintTopologyJsonFormat() throws Exception {
    // Get http Address.
    String httpAddress = clusterWithDatanodes.getRandomRouter().getRouter()
        .getHttpServerAddress().toString();

    // Send http request.
    URL url = new URL("http:/" + httpAddress + "/topology");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setReadTimeout(20000);
    conn.setConnectTimeout(20000);
    conn.setRequestProperty("Accept", "application/json");
    conn.connect();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
    String topology = out.toString();

    // Parse json.
    JsonNode racks = new ObjectMapper().readTree(topology);

    // Assert rack number.
    assertEquals(6, racks.size());

    // Assert rack info.
    assertTrue(topology.contains("/ns0/rack1"));
    assertTrue(topology.contains("/ns0/rack2"));
    assertTrue(topology.contains("/ns0/rack3"));
    assertTrue(topology.contains("/ns1/rack4"));
    assertTrue(topology.contains("/ns1/rack5"));
    assertTrue(topology.contains("/ns1/rack6"));

    // Assert node number.
    Iterator<JsonNode> elements = racks.elements();
    int dataNodesCount = 0;
    while(elements.hasNext()){
      JsonNode rack = elements.next();
      Iterator<Map.Entry<String, JsonNode>> fields = rack.fields();
      while (fields.hasNext()) {
        dataNodesCount += fields.next().getValue().size();
      }
    }
    assertEquals(18, dataNodesCount);
  }

  public void testPrintTopologyNoDatanodesTextFormat() throws Exception {
    // Get http Address.
    String httpAddress = clusterNoDatanodes.getRandomRouter().getRouter()
        .getHttpServerAddress().toString();

    // Send http request.
    URL url = new URL("http:/" + httpAddress + "/topology");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setReadTimeout(20000);
    conn.setConnectTimeout(20000);
    conn.connect();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
    StringBuilder sb =
        new StringBuilder("-- Network Topology -- \n");
    sb.append(out);
    sb.append("\n-- Network Topology -- ");
    String topology = sb.toString();

    // Assert node number.
    assertTrue(topology.contains("No DataNodes"));
  }

  public void testPrintTopologyNoDatanodesJsonFormat() throws Exception {
    // Get http Address.
    String httpAddress = clusterNoDatanodes.getRandomRouter().getRouter()
        .getHttpServerAddress().toString();

    // Send http request.
    URL url = new URL("http:/" + httpAddress + "/topology");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setReadTimeout(20000);
    conn.setConnectTimeout(20000);
    conn.setRequestProperty("Accept", "application/json");
    conn.connect();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
    StringBuilder sb =
        new StringBuilder("-- Network Topology -- \n");
    sb.append(out);
    sb.append("\n-- Network Topology -- ");
    String topology = sb.toString();

    // Assert node number.
    assertTrue(topology.contains("No DataNodes"));
  }
}

class RouterServerHelper implements BeforeEachCallback, AfterAllCallback {

  private static final ThreadLocal<RouterServerHelper> TEST_ROUTER_SERVER_TL =
      new InheritableThreadLocal<RouterServerHelper>();

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    Method testMethod = context.getRequiredTestMethod();
    ValueSource enumAnnotation = testMethod.getAnnotation(ValueSource.class);
    if (enumAnnotation != null) {
      String[] strings = enumAnnotation.strings();
      for (String rpcMode : strings) {
        if (TEST_ROUTER_SERVER_TL.get() == null) {
          setUp(rpcMode);
        }
      }
    }
    TEST_ROUTER_SERVER_TL.set(RouterServerHelper.this);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (clusterWithDatanodes != null) {
      clusterWithDatanodes.shutdown();
      clusterWithDatanodes = null;
    }
    if (clusterNoDatanodes != null) {
      clusterNoDatanodes.shutdown();
      clusterNoDatanodes = null;
    }
    TEST_ROUTER_SERVER_TL.remove();
  }
}
