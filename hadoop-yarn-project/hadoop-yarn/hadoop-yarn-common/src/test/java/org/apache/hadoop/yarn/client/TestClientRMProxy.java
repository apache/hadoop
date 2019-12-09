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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

  /**
   * Verify that the RPC layer is always created using the correct UGI from the
   * RMProxy.  It should always use the UGI from creation in subsequent uses,
   * even outside of a doAs.
   *
   * @throws Exception an Exception occurred
   */
  @Test
  public void testProxyUserCorrectUGI() throws Exception {
    final YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, "rm1"),
        "0.0.0.0");
    conf.set(HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, "rm2"),
        "0.0.0.0");
    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_MAX_ATTEMPTS, 2);
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, 2);
    conf.setLong(
        YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 2);

    // Replace the RPC implementation with one that will capture the current UGI
    conf.setClass(YarnConfiguration.IPC_RPC_IMPL,
        UGICapturingHadoopYarnProtoRPC.class, YarnRPC.class);

    UserGroupInformation realUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation proxyUser =
        UserGroupInformation.createProxyUserForTesting("proxy", realUser,
        new String[] {"group1"});

    // Create the RMProxy using the proxyUser
    ApplicationClientProtocol rmProxy = proxyUser.doAs(
        new PrivilegedExceptionAction<ApplicationClientProtocol>() {
          @Override
          public ApplicationClientProtocol run() throws Exception {
            return ClientRMProxy.createRMProxy(conf,
                ApplicationClientProtocol.class);
          }
        });

    // It was in a doAs, so the UGI should be correct
    assertUGI();

    // Try to use the RMProxy, which should trigger the RPC again
    GetNewApplicationRequest request =
        Records.newRecord(GetNewApplicationRequest.class);
    UGICapturingHadoopYarnProtoRPC.lastCurrentUser = null;
    try {
      rmProxy.getNewApplication(request);
    } catch (IOException ioe) {
      // ignore - RMs are not running so this is expected to fail
    }

    // This time it was outside a doAs, but make sure the UGI was still correct
    assertUGI();
  }

  private void assertUGI() throws IOException {
    UserGroupInformation lastCurrentUser =
        UGICapturingHadoopYarnProtoRPC.lastCurrentUser;
    assertNotNull(lastCurrentUser);
    assertEquals("proxy", lastCurrentUser.getShortUserName());
    Assert.assertEquals(UserGroupInformation.AuthenticationMethod.PROXY,
        lastCurrentUser.getAuthenticationMethod());
    assertEquals(UserGroupInformation.getCurrentUser(),
        lastCurrentUser.getRealUser());
    // Reset UGICapturingHadoopYarnProtoRPC
    UGICapturingHadoopYarnProtoRPC.lastCurrentUser = null;
  }

  /**
   * Subclass of {@link HadoopYarnProtoRPC} which captures the current UGI in
   * a static variable.  Used by {@link #testProxyUserCorrectUGI()}.
   */
  public static class UGICapturingHadoopYarnProtoRPC
      extends HadoopYarnProtoRPC {

    static UserGroupInformation lastCurrentUser = null;

    @Override
    public Object getProxy(Class protocol, InetSocketAddress addr,
        Configuration conf) {
      UserGroupInformation currentUser = null;
      try {
        currentUser = UserGroupInformation.getCurrentUser();
      } catch (IOException ioe) {
        Assert.fail("Unable to get current user\n"
            + StringUtils.stringifyException(ioe));
      }
      lastCurrentUser = currentUser;

      return super.getProxy(protocol, addr, conf);
    }
  }
}
