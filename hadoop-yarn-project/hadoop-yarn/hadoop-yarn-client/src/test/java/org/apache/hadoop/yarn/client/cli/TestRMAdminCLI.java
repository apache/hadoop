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

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.cli.RMAdminCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.DummyCommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;

public class TestRMAdminCLI {

  private ResourceManagerAdministrationProtocol admin;
  private HAServiceProtocol haadmin;
  private RMAdminCLI rmAdminCLI;
  private RMAdminCLI rmAdminCLIWithHAEnabled;
  private CommonNodeLabelsManager dummyNodeLabelsManager;
  private boolean remoteAdminServiceAccessed = false;

  @SuppressWarnings("static-access")
  @Before
  public void configure() throws IOException, YarnException {
    remoteAdminServiceAccessed = false;
    admin = mock(ResourceManagerAdministrationProtocol.class);
    when(admin.addToClusterNodeLabels(any(AddToClusterNodeLabelsRequest.class)))
        .thenAnswer(new Answer<AddToClusterNodeLabelsResponse>() {

          @Override
          public AddToClusterNodeLabelsResponse answer(
              InvocationOnMock invocation) throws Throwable {
            remoteAdminServiceAccessed = true;
            return AddToClusterNodeLabelsResponse.newInstance();
          }
        });

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
    initDummyNodeLabelsManager();
    rmAdminCLI.localNodeLabelsManager = dummyNodeLabelsManager;

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
  
  private void initDummyNodeLabelsManager() {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    dummyNodeLabelsManager = new DummyCommonNodeLabelsManager();
    dummyNodeLabelsManager.init(conf);
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
              " [username]] [[-addToClusterNodeLabels [label1,label2,label3]]" +
              " [-removeFromClusterNodeLabels [label1,label2,label3]] [-replaceLabelsOnNode " +
              "[node1[:port]=label1,label2 node2[:port]=label1] [-directlyAccessNodeLabelStore]] " +
              "[-help [cmd]]"));
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
          "Usage: yarn rmadmin [-transitionToActive [--forceactive]" +
          " <serviceId>]", dataErr, 0);
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
      String expectedHelpMsg = 
          "yarn rmadmin [-refreshQueues] [-refreshNodes] [-refreshSuper" +
              "UserGroupsConfiguration] [-refreshUserToGroupsMappings] " +
              "[-refreshAdminAcls] [-refreshServiceAcl] [-getGroup" +
              " [username]] [[-addToClusterNodeLabels [label1,label2,label3]]" +
              " [-removeFromClusterNodeLabels [label1,label2,label3]] [-replaceLabelsOnNode " +
              "[node1[:port]=label1,label2 node2[:port]=label1] [-directlyAccessNodeLabelStore]] " +
              "[-transitionToActive [--forceactive] <serviceId>] " + 
              "[-transitionToStandby <serviceId>] [-failover" +
              " [--forcefence] [--forceactive] <serviceId> <serviceId>] " +
              "[-getServiceState <serviceId>] [-checkHealth <serviceId>] [-help [cmd]]";
      String actualHelpMsg = dataOut.toString();
      assertTrue(String.format("Help messages: %n " + actualHelpMsg + " %n doesn't include expected " +
          "messages: %n" + expectedHelpMsg), actualHelpMsg.contains(expectedHelpMsg
              ));
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
  
  @Test
  public void testAccessLocalNodeLabelManager() throws Exception {
    assertFalse(dummyNodeLabelsManager.getServiceState() == STATE.STOPPED);
    
    String[] args =
        { "-addToClusterNodeLabels", "x,y", "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabels().containsAll(
        ImmutableSet.of("x", "y")));
    
    // reset localNodeLabelsManager
    dummyNodeLabelsManager.removeFromClusterNodeLabels(ImmutableSet.of("x", "y"));
    
    // change the sequence of "-directlyAccessNodeLabelStore" and labels,
    // should not matter
    args =
        new String[] { "-addToClusterNodeLabels",
            "-directlyAccessNodeLabelStore", "x,y" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabels().containsAll(
        ImmutableSet.of("x", "y")));
    
    // local node labels manager will be close after running
    assertTrue(dummyNodeLabelsManager.getServiceState() == STATE.STOPPED);
  }
  
  @Test
  public void testAccessRemoteNodeLabelManager() throws Exception {
    String[] args =
        { "-addToClusterNodeLabels", "x,y" };
    assertEquals(0, rmAdminCLI.run(args));
    
    // localNodeLabelsManager shouldn't accessed
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabels().isEmpty());
    
    // remote node labels manager accessed
    assertTrue(remoteAdminServiceAccessed);
  }
  
  @Test
  public void testAddToClusterNodeLabels() throws Exception {
    // successfully add labels
    String[] args =
        { "-addToClusterNodeLabels", "x", "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabels().containsAll(
        ImmutableSet.of("x")));
    
    // no labels, should fail
    args = new String[] { "-addToClusterNodeLabels" };
    assertTrue(0 != rmAdminCLI.run(args));
    
    // no labels, should fail
    args =
        new String[] { "-addToClusterNodeLabels",
            "-directlyAccessNodeLabelStore" };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail at client validation
    args = new String[] { "-addToClusterNodeLabels", " " };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail at client validation
    args = new String[] { "-addToClusterNodeLabels", " , " };
    assertTrue(0 != rmAdminCLI.run(args));

    // successfully add labels
    args =
        new String[] { "-addToClusterNodeLabels", ",x,,",
            "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabels().containsAll(
        ImmutableSet.of("x")));
  }
  
  @Test
  public void testRemoveFromClusterNodeLabels() throws Exception {
    // Successfully remove labels
    dummyNodeLabelsManager.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    String[] args =
        { "-removeFromClusterNodeLabels", "x,,y",
            "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabels().isEmpty());
    
    // no labels, should fail
    args = new String[] { "-removeFromClusterNodeLabels" };
    assertTrue(0 != rmAdminCLI.run(args));
    
    // no labels, should fail
    args =
        new String[] { "-removeFromClusterNodeLabels",
            "-directlyAccessNodeLabelStore" };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail at client validation
    args = new String[] { "-removeFromClusterNodeLabels", " " };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail at client validation
    args = new String[] { "-removeFromClusterNodeLabels", ", " };
    assertTrue(0 != rmAdminCLI.run(args));
  }
  
  @Test
  public void testReplaceLabelsOnNode() throws Exception {
    // Successfully replace labels
    dummyNodeLabelsManager
        .addToCluserNodeLabels(ImmutableSet.of("x", "y", "Y"));
    String[] args =
        { "-replaceLabelsOnNode",
            "node1:8000,x node2:8000=y node3,x node4=Y",
            "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getNodeLabels().containsKey(
        NodeId.newInstance("node1", 8000)));
    assertTrue(dummyNodeLabelsManager.getNodeLabels().containsKey(
        NodeId.newInstance("node2", 8000)));
    assertTrue(dummyNodeLabelsManager.getNodeLabels().containsKey(
        NodeId.newInstance("node3", 0)));
    assertTrue(dummyNodeLabelsManager.getNodeLabels().containsKey(
        NodeId.newInstance("node4", 0)));

    // no labels, should fail
    args = new String[] { "-replaceLabelsOnNode" };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail
    args =
        new String[] { "-replaceLabelsOnNode", "-directlyAccessNodeLabelStore" };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail
    args = new String[] { "-replaceLabelsOnNode", " " };
    assertTrue(0 != rmAdminCLI.run(args));

    args = new String[] { "-replaceLabelsOnNode", ", " };
    assertTrue(0 != rmAdminCLI.run(args));
  }
  
  @Test
  public void testReplaceMultipleLabelsOnSingleNode() throws Exception {
    // Successfully replace labels
    dummyNodeLabelsManager.addToCluserNodeLabels(ImmutableSet.of("x", "y"));
    String[] args =
        { "-replaceLabelsOnNode", "node1,x,y",
            "-directlyAccessNodeLabelStore" };
    assertTrue(0 != rmAdminCLI.run(args));
  }

  private void testError(String[] args, String template,
      ByteArrayOutputStream data, int resultCode) throws Exception {
    int actualResultCode = rmAdminCLI.run(args);
    assertEquals("Expected result code: " + resultCode + 
        ", actual result code is: " + actualResultCode, resultCode, actualResultCode);
    assertTrue(String.format("Expected error message: %n" + template + 
        " is not included in messages: %n" + data.toString()), 
        data.toString().contains(template));
    data.reset();
  }

  @Test
  public void testRMHAErrorUsage() throws Exception {
    ByteArrayOutputStream errOutBytes = new ByteArrayOutputStream();
    rmAdminCLIWithHAEnabled.setErrOut(new PrintStream(errOutBytes));
    try {
      String[] args = { "-failover" };
      assertEquals(-1, rmAdminCLIWithHAEnabled.run(args));
      String errOut = new String(errOutBytes.toByteArray(), Charsets.UTF_8);
      errOutBytes.reset();
      assertTrue(errOut.contains("Usage: rmadmin"));
    } finally {
      rmAdminCLIWithHAEnabled.setErrOut(System.err);
    }
  }

}
