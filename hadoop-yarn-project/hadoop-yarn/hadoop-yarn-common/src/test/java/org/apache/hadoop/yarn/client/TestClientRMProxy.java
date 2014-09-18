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

package org.apache.hadoop.yarn.client;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestClientRMProxy {
  @Test
  public void testGetRMDelegationTokenService() {
    String defaultRMAddress = YarnConfiguration.DEFAULT_RM_ADDRESS;
    YarnConfiguration conf = new YarnConfiguration();

    // HA is not enabled
    Text tokenService = ClientRMProxy.getRMDelegationTokenService(conf);
    String[] services = tokenService.toString().split(",");
    assertEquals(1, services.length);
    for (String service : services) {
      assertTrue("Incorrect token service name",
          service.contains(defaultRMAddress));
    }

    // HA is enabled
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, "rm1"),
        "0.0.0.0");
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, "rm2"),
        "0.0.0.0");
    tokenService = ClientRMProxy.getRMDelegationTokenService(conf);
    services = tokenService.toString().split(",");
    assertEquals(2, services.length);
    for (String service : services) {
      assertTrue("Incorrect token service name",
          service.contains(defaultRMAddress));
    }
  }

  @Test
  public void testGetAMRMTokenService() {
    String defaultRMAddress = YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS;
    YarnConfiguration conf = new YarnConfiguration();

    // HA is not enabled
    Text tokenService = ClientRMProxy.getAMRMTokenService(conf);
    String[] services = tokenService.toString().split(",");
    assertEquals(1, services.length);
    for (String service : services) {
      assertTrue("Incorrect token service name",
          service.contains(defaultRMAddress));
    }

    // HA is enabled
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, "rm1"),
        "0.0.0.0");
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, "rm2"),
        "0.0.0.0");
    tokenService = ClientRMProxy.getAMRMTokenService(conf);
    services = tokenService.toString().split(",");
    assertEquals(2, services.length);
    for (String service : services) {
      assertTrue("Incorrect token service name",
          service.contains(defaultRMAddress));
    }
  }
}
