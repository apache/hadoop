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

package org.apache.hadoop.yarn.client.cli;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for TopCli.
 *
 */
public class TestTopCLI {

  private static final String RM1_NODE_ID = "rm1";
  private static final String RM2_NODE_ID = "rm2";

  private static List<String> dummyHostNames =
      Arrays.asList("host1", "host2", "host3");

  private static Map<String, String> savedStaticResolution = new HashMap<>();

  private PrintStream stdout;
  private PrintStream stderr;

  @BeforeAll
  public static void initializeDummyHostnameResolution() throws Exception {
    String previousIpAddress;
    for (String hostName : dummyHostNames) {
      previousIpAddress = NetUtils.getStaticResolution(hostName);
      if (null != previousIpAddress) {
        savedStaticResolution.put(hostName, previousIpAddress);
      }
      NetUtils.addStaticResolution(hostName, "10.20.30.40");
    }
  }

  @AfterAll
  public static void restoreDummyHostnameResolution() throws Exception {
    for (Map.Entry<String, String> hostnameToIpEntry : savedStaticResolution
        .entrySet()) {
      NetUtils.addStaticResolution(hostnameToIpEntry.getKey(),
          hostnameToIpEntry.getValue());
    }
  }

  @BeforeEach
  public void before() {
    this.stdout = System.out;
    this.stderr = System.err;
  }

  @AfterEach
  public void after() {
    System.setOut(this.stdout);
    System.setErr(this.stderr);
  }

  @Test
  public void testHAClusterInfoURL() throws IOException, InterruptedException {
    TopCLI topcli = new TopCLI();
    // http
    String rm1Address = "host2:8088";
    String rm2Address = "host3:8088";
    Configuration conf = topcli.getConf();
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS + "." + RM1_NODE_ID,
        rm1Address);
    topcli.getConf().set(
        YarnConfiguration.RM_WEBAPP_ADDRESS + "." + RM2_NODE_ID, rm2Address);
    topcli.getConf().setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    topcli.getConf().set(YarnConfiguration.RM_HA_IDS,
        RM1_NODE_ID + "," + RM2_NODE_ID);
    URL clusterUrl = topcli.getHAClusterUrl(conf, RM1_NODE_ID);
    assertEquals("http", clusterUrl.getProtocol());
    assertEquals(rm1Address, clusterUrl.getAuthority());
    clusterUrl = topcli.getHAClusterUrl(conf, RM2_NODE_ID);
    assertEquals("http", clusterUrl.getProtocol());
    assertEquals(rm2Address, clusterUrl.getAuthority());
    // https
    rm1Address = "host2:9088";
    rm2Address = "host3:9088";
    conf = topcli.getConf();
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS + "." + RM1_NODE_ID,
        rm1Address);
    conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS + "." + RM2_NODE_ID,
        rm2Address);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY, "HTTPS_ONLY");
    clusterUrl = topcli.getHAClusterUrl(conf, RM1_NODE_ID);
    assertEquals("https", clusterUrl.getProtocol());
    assertEquals(rm1Address, clusterUrl.getAuthority());
  }

  @Test
  public void testHeaderNodeManagers() throws Exception {
    YarnClusterMetrics ymetrics = mock(YarnClusterMetrics.class);
    when(ymetrics.getNumNodeManagers()).thenReturn(0);
    when(ymetrics.getNumDecommissioningNodeManagers()).thenReturn(1);
    when(ymetrics.getNumDecommissionedNodeManagers()).thenReturn(2);
    when(ymetrics.getNumActiveNodeManagers()).thenReturn(3);
    when(ymetrics.getNumLostNodeManagers()).thenReturn(4);
    when(ymetrics.getNumUnhealthyNodeManagers()).thenReturn(5);
    when(ymetrics.getNumRebootedNodeManagers()).thenReturn(6);
    when(ymetrics.getNumShutdownNodeManagers()).thenReturn(7);

    YarnClient client = mock(YarnClient.class);
    when(client.getYarnClusterMetrics()).thenReturn(ymetrics);

    TopCLI topcli = new TopCLI() {
      @Override protected void createAndStartYarnClient() {
      }
    };
    topcli.setClient(client);
    topcli.terminalWidth = 200;

    String actual;
    try (ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outStream)) {
      System.setOut(out);
      System.setErr(out);
      topcli.showTopScreen();
      out.flush();
      actual = outStream.toString(StandardCharsets.UTF_8.name());
    }

    String expected = "NodeManager(s)"
        + ": 0 total, 3 active, 5 unhealthy, 1 decommissioning,"
        + " 2 decommissioned, 4 lost, 6 rebooted, 7 shutdown";
    assertTrue(actual.contains(expected),
        String.format("Expected output to contain [%s], actual output was [%s].",
        expected, actual));
  }
}
