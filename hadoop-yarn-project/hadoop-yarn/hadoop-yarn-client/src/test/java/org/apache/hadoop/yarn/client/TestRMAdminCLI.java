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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.yarn.client.cli.RMAdminCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

public class TestRMAdminCLI {

  private ResourceManagerAdministrationProtocol admin;
  private HAServiceProtocol haadmin;
  private RMAdminCLI rmAdminCLI;
  private RMAdminCLI rmAdminCLIWithHAEnabled;

  @Before
  public void configure() throws IOException {
    admin = mock(ResourceManagerAdministrationProtocol.class);

    haadmin = mock(HAServiceProtocol.class);
    when(haadmin.getServiceStatus()).thenReturn(new HAServiceStatus(
        HAServiceProtocol.HAServiceState.INITIALIZING));

    final HAServiceTarget haServiceTarget = mock(HAServiceTarget.class);
    when(haServiceTarget.getProxy(any(Configuration.class), anyInt()))
        .thenReturn(haadmin);
    rmAdminCLI = new RMAdminCLI(new Configuration()) {

      @Override
      protected ResourceManagerAdministrationProtocol createAdminProtocol()
          throws IOException {
        return admin;
      }

      @Override
      protected HAServiceTarget resolveTarget(String rmId) {
        return haServiceTarget;
      }
    };

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    rmAdminCLIWithHAEnabled = new RMAdminCLI(conf) {

      @Override
      protected ResourceManagerAdministrationProtocol createAdminProtocol()
          throws IOException {
        return admin;
      }

      @Override
      protected HAServiceTarget resolveTarget(String rmId) {
        return haServiceTarget;
      }
    };
  }
  
  @Test(timeout=500)
  public void testRefreshQueues() throws Exception {
    String[] args = { "-refreshQueues" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshQueues(any(RefreshQueuesRequest.class));
  }

  @Test(timeout=500)
  public void testRefreshUserToGroupsMappings() throws Exception {
    String[] args = { "-refreshUserToGroupsMappings" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshUserToGroupsMappings(
        any(RefreshUserToGroupsMappingsRequest.class));
  }

  @Test(timeout=500)
  public void testRefreshSuperUserGroupsConfiguration() throws Exception {
    String[] args = { "-refreshSuperUserGroupsConfiguration" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshSuperUserGroupsConfiguration(
        any(RefreshSuperUserGroupsConfigurationRequest.class));
  }

  @Test(timeout=500)
  public void testRefreshAdminAcls() throws Exception {
    String[] args = { "-refreshAdminAcls" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshAdminAcls(any(RefreshAdminAclsRequest.class));
  }

  @Test(timeout=500)
  public void testRefreshServiceAcl() throws Exception {
    String[] args = { "-refreshServiceAcl" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshServiceAcls(any(RefreshServiceAclsRequest.class));
  }

  @Test(timeout=500)
  public void testRefreshNodes() throws Exception {
    String[] args = { "-refreshNodes" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshNodes(any(RefreshNodesRequest.class));
  }
  
  @Test(timeout=500)
  public void testGetGroups() throws Exception {
    when(admin.getGroupsForUser(eq("admin"))).thenReturn(
        new String[] {"group1", "group2"});
    PrintStream origOut = System.out;
    PrintStream out = mock(PrintStream.class);
    System.setOut(out);
    try {
      String[] args = { "-getGroups", "admin" };
      assertEquals(0, rmAdminCLI.run(args));
      verify(admin).getGroupsForUser(eq("admin"));
      verify(out).println(argThat(new ArgumentMatcher<StringBuilder>() {
        @Override
        public boolean matches(Object argument) {
          return ("" + argument).equals("admin : group1 group2");
        }
      }));
    } finally {
      System.setOut(origOut);
    }
  }

  @Test(timeout = 500)
  public void testTransitionToActive() throws Exception {
    String[] args = {"-transitionToActive", "rm1"};

    // RM HA is disabled.
    // transitionToActive should not be executed
    assertEquals(-1, rmAdminCLI.run(args));
    verify(haadmin, never()).transitionToActive(
        any(HAServiceProtocol.StateChangeRequestInfo.class));

    // Now RM HA is enabled.
    // transitionToActive should be executed
    assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
    verify(haadmin).transitionToActive(
        any(HAServiceProtocol.StateChangeRequestInfo.class));
  }

  @Test(timeout = 500)
  public void testTransitionToStandby() throws Exception {
    String[] args = {"-transitionToStandby", "rm1"};

    // RM HA is disabled.
    // transitionToStandby should not be executed
    assertEquals(-1, rmAdminCLI.run(args));
    verify(haadmin, never()).transitionToStandby(
        any(HAServiceProtocol.StateChangeRequestInfo.class));

    // Now RM HA is enabled.
    // transitionToActive should be executed
    assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
    verify(haadmin).transitionToStandby(
        any(HAServiceProtocol.StateChangeRequestInfo.class));
  }

  @Test(timeout = 500)
  public void testGetServiceState() throws Exception {
    String[] args = {"-getServiceState", "rm1"};

    // RM HA is disabled.
    // getServiceState should not be executed
    assertEquals(-1, rmAdminCLI.run(args));
    verify(haadmin, never()).getServiceStatus();

    // Now RM HA is enabled.
    // getServiceState should be executed
    assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
    verify(haadmin).getServiceStatus();
  }

  @Test(timeout = 500)
  public void testCheckHealth() throws Exception {
    String[] args = {"-checkHealth", "rm1"};

    // RM HA is disabled.
    // getServiceState should not be executed
    assertEquals(-1, rmAdminCLI.run(args));
    verify(haadmin, never()).monitorHealth();

    // Now RM HA is enabled.
    // getServiceState should be executed
    assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
    verify(haadmin).monitorHealth();
  }

  /**
   * Test printing of help messages
   */
  @Test(timeout=500)
  public void testHelp() throws Exception {
    PrintStream oldOutPrintStream = System.out;
    PrintStream oldErrPrintStream = System.err;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    System.setErr(new PrintStream(dataErr));
    try {
      String[] args = { "-help" };
      assertEquals(0, rmAdminCLI.run(args));
      oldOutPrintStream.println(dataOut);
      assertTrue(dataOut
          .toString()
          .contains(
              "rmadmin is the command to execute YARN administrative commands."));
      assertTrue(dataOut
          .toString()
          .contains(
              "yarn rmadmin [-refreshQueues] [-refreshNodes] [-refreshSuper" +
              "UserGroupsConfiguration] [-refreshUserToGroupsMappings] " +
              "[-refreshAdminAcls] [-refreshServiceAcl] [-getGroup" +
              " [username]] [-help [cmd]]"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshQueues: Reload the queues' acls, states and scheduler " +
              "specific properties."));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshNodes: Refresh the hosts information at the " +
              "ResourceManager."));
      assertTrue(dataOut.toString().contains(
          "-refreshUserToGroupsMappings: Refresh user-to-groups mappings"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy" +
              " groups mappings"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshAdminAcls: Refresh acls for administration of " +
              "ResourceManager"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshServiceAcl: Reload the service-level authorization" +
              " policy file"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-help [cmd]: Displays help for the given command or all " +
              "commands if none"));

      testError(new String[] { "-help", "-refreshQueues" },
          "Usage: yarn rmadmin [-refreshQueues]", dataErr, 0);
      testError(new String[] { "-help", "-refreshNodes" },
          "Usage: yarn rmadmin [-refreshNodes]", dataErr, 0);
      testError(new String[] { "-help", "-refreshUserToGroupsMappings" },
          "Usage: yarn rmadmin [-refreshUserToGroupsMappings]", dataErr, 0);
      testError(
          new String[] { "-help", "-refreshSuperUserGroupsConfiguration" },
          "Usage: yarn rmadmin [-refreshSuperUserGroupsConfiguration]",
          dataErr, 0);
      testError(new String[] { "-help", "-refreshAdminAcls" },
          "Usage: yarn rmadmin [-refreshAdminAcls]", dataErr, 0);
      testError(new String[] { "-help", "-refreshServiceAcl" },
          "Usage: yarn rmadmin [-refreshServiceAcl]", dataErr, 0);
      testError(new String[] { "-help", "-getGroups" },
          "Usage: yarn rmadmin [-getGroups [username]]", dataErr, 0);
      testError(new String[] { "-help", "-transitionToActive" },
          "Usage: yarn rmadmin [-transitionToActive <serviceId>" +
          " [--forceactive]]", dataErr, 0);
      testError(new String[] { "-help", "-transitionToStandby" },
          "Usage: yarn rmadmin [-transitionToStandby <serviceId>]", dataErr, 0);
      testError(new String[] { "-help", "-getServiceState" },
          "Usage: yarn rmadmin [-getServiceState <serviceId>]", dataErr, 0);
      testError(new String[] { "-help", "-checkHealth" },
          "Usage: yarn rmadmin [-checkHealth <serviceId>]", dataErr, 0);
      testError(new String[] { "-help", "-failover" },
          "Usage: yarn rmadmin " +
              "[-failover [--forcefence] [--forceactive] " +
              "<serviceId> <serviceId>]",
          dataErr, 0);

      testError(new String[] { "-help", "-badParameter" },
          "Usage: yarn rmadmin", dataErr, 0);
      testError(new String[] { "-badParameter" },
          "badParameter: Unknown command", dataErr, -1);

      // Test -help when RM HA is enabled
      assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
      oldOutPrintStream.println(dataOut);
      assertTrue(dataOut
          .toString()
          .contains(
              "yarn rmadmin [-refreshQueues] [-refreshNodes] [-refreshSuper" +
              "UserGroupsConfiguration] [-refreshUserToGroupsMappings] " +
              "[-refreshAdminAcls] [-refreshServiceAcl] [-getGroup" +
              " [username]] [-help [cmd]] [-transitionToActive <serviceId>" + 
              " [--forceactive]] [-transitionToStandby <serviceId>] [-failover" +
              " [--forcefence] [--forceactive] <serviceId> <serviceId>] " +
              "[-getServiceState <serviceId>] [-checkHealth <serviceId>]"));
    } finally {
      System.setOut(oldOutPrintStream);
      System.setErr(oldErrPrintStream);
    }
  }

  @Test(timeout=500)
  public void testException() throws Exception {
    PrintStream oldErrPrintStream = System.err;
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(dataErr));
    try {
      when(admin.refreshQueues(any(RefreshQueuesRequest.class)))
          .thenThrow(new IOException("test exception"));
      String[] args = { "-refreshQueues" };

      assertEquals(-1, rmAdminCLI.run(args));
      verify(admin).refreshQueues(any(RefreshQueuesRequest.class));
      assertTrue(dataErr.toString().contains("refreshQueues: test exception"));
    } finally {
      System.setErr(oldErrPrintStream);
    }
  }

  private void testError(String[] args, String template,
      ByteArrayOutputStream data, int resultCode) throws Exception {
    assertEquals(resultCode, rmAdminCLI.run(args));
    assertTrue(data.toString().contains(template));
    data.reset();
  }
  
}
