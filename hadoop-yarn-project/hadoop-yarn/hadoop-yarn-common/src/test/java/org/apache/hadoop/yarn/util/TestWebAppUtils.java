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

package org.apache.hadoop.yarn.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class TestWebAppUtils {

  private static final String RM1_NODE_ID = "rm1";
  private static final String RM2_NODE_ID = "rm2";

  // Because WebAppUtils#getResolvedAddress tries to resolve the hostname, we add a static mapping for dummy hostnames
  // to make this test run anywhere without having to give some resolvable hostnames
  private static String dummyHostNames[] = {"host1", "host2", "host3"};
  private static final String anyIpAddress = "1.2.3.4";
  private static Map<String, String> savedStaticResolution = new HashMap<>();

  @BeforeClass
  public static void initializeDummyHostnameResolution() throws Exception {
    String previousIpAddress;
    for (String hostName : dummyHostNames) {
      if (null != (previousIpAddress = NetUtils.getStaticResolution(hostName))) {
        savedStaticResolution.put(hostName, previousIpAddress);
      }
      NetUtils.addStaticResolution(hostName, anyIpAddress);
    }
  }

  @AfterClass
  public static void restoreDummyHostnameResolution() throws Exception {
    for (Map.Entry<String, String> hostnameToIpEntry : savedStaticResolution.entrySet()) {
      NetUtils.addStaticResolution(hostnameToIpEntry.getKey(), hostnameToIpEntry.getValue());
    }
  }

  @Test
  public void TestRMWebAppURLRemoteAndLocal() throws UnknownHostException {
    Configuration configuration = new Configuration();
    final String rmAddress = "host1:8088";
    configuration.set(YarnConfiguration.RM_WEBAPP_ADDRESS, rmAddress);
    final String rm1Address = "host2:8088";
    final String rm2Address = "host3:8088";
    configuration.set(YarnConfiguration.RM_WEBAPP_ADDRESS + "." + RM1_NODE_ID, rm1Address);
    configuration.set(YarnConfiguration.RM_WEBAPP_ADDRESS + "." + RM2_NODE_ID, rm2Address);
    configuration.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    configuration.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);

    String rmRemoteUrl = WebAppUtils.getResolvedRemoteRMWebAppURLWithoutScheme(configuration);
    Assert.assertEquals("ResolvedRemoteRMWebAppUrl should resolve to the first HA RM address", rm1Address, rmRemoteUrl);

    String rmLocalUrl = WebAppUtils.getResolvedRMWebAppURLWithoutScheme(configuration);
    Assert.assertEquals("ResolvedRMWebAppUrl should resolve to the default RM webapp address", rmAddress, rmLocalUrl);
  }
}
